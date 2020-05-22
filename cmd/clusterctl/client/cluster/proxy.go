/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cluster

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/cluster-api/cmd/clusterctl/internal/scheme"
	"sigs.k8s.io/cluster-api/cmd/version"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	Scheme = scheme.Scheme
)

type proxy struct {
	kubeconfig string
}

var _ Proxy = &proxy{}

func (k *proxy) CurrentNamespace() (string, error) {
	config, err := clientcmd.LoadFromFile(k.kubeconfig)
	if err != nil {
		return "", errors.Wrapf(err, "failed to load Kubeconfig file from %q", k.kubeconfig)
	}

	if config.CurrentContext == "" {
		return "", errors.Wrapf(err, "failed to get current-context from %q", k.kubeconfig)
	}

	v, ok := config.Contexts[config.CurrentContext]
	if !ok {
		return "", errors.Wrapf(err, "failed to get context %q from %q", config.CurrentContext, k.kubeconfig)
	}

	if v.Namespace != "" {
		return v.Namespace, nil
	}

	return "default", nil
}

func (k *proxy) NewClient() (client.Client, error) {
	config, err := k.getConfig()
	if err != nil {
		return nil, err
	}

	var c client.Client
	// Nb. The operation is wrapped in a retry loop to make newClientSet more resilient to temporary connection problems.
	connectBackoff := newConnectBackoff()
	if err := retryWithExponentialBackoff(connectBackoff, func() error {
		var err error
		c, err = client.New(config, client.Options{Scheme: Scheme})
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "failed to connect to the management cluster")
	}

	return c, nil
}

func (k *proxy) ListResources(labels map[string]string, namespaces ...string) ([]unstructured.Unstructured, error) {
	cs, err := k.newClientSet()
	if err != nil {
		return nil, err
	}

	c, err := k.NewClient()
	if err != nil {
		return nil, err
	}

	// Get all the API resources in the cluster.
	resourceList, err := cs.Discovery().ServerPreferredResources()
	if err != nil {
		return nil, errors.Wrap(err, "failed to list api resources")
	}

	// Select resources with list and delete methods (list is required by this method, delete by the callers of this method)
	resourceList = discovery.FilteredBy(discovery.SupportsAllVerbs{Verbs: []string{"list", "delete"}}, resourceList)

	var ret []unstructured.Unstructured
	for _, resourceGroup := range resourceList {
		for _, resourceKind := range resourceGroup.APIResources {
			// Discard the resourceKind that exists in two api groups (we are excluding one of the two groups arbitrarily).
			if resourceGroup.GroupVersion == "extensions/v1beta1" &&
				(resourceKind.Name == "daemonsets" || resourceKind.Name == "deployments" || resourceKind.Name == "replicasets" || resourceKind.Name == "networkpolicies" || resourceKind.Name == "ingresses") {
				continue
			}

			// List all the object instances of this resourceKind with the given labels
			if resourceKind.Namespaced {
				for _, namespace := range namespaces {
					objList, err := listObjByGVK(c, resourceGroup.GroupVersion, resourceKind.Kind, []client.ListOption{client.MatchingLabels(labels), client.InNamespace(namespace)})
					if err != nil {
						return nil, err
					}
					ret = append(ret, objList.Items...)
				}
			} else {
				objList, err := listObjByGVK(c, resourceGroup.GroupVersion, resourceKind.Kind, []client.ListOption{client.MatchingLabels(labels)})
				if err != nil {
					return nil, err
				}
				ret = append(ret, objList.Items...)
			}
		}
	}
	return ret, nil
}

func listObjByGVK(c client.Client, groupVersion, kind string, options []client.ListOption) (*unstructured.UnstructuredList, error) {
	objList := new(unstructured.UnstructuredList)
	objList.SetAPIVersion(groupVersion)
	objList.SetKind(kind)

	if err := c.List(ctx, objList, options...); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, errors.Wrapf(err, "failed to list objects for the %q GroupVersionKind", objList.GroupVersionKind())
		}
	}
	return objList, nil
}

func newProxy(kubeconfig string) Proxy {
	// If a kubeconfig file isn't provided, find one in the standard locations.
	if kubeconfig == "" {
		kubeconfig = clientcmd.NewDefaultClientConfigLoadingRules().GetDefaultFilename()
	}
	return &proxy{
		kubeconfig: kubeconfig,
	}
}

func getRestConfig(kubeconfigFile string) (*rest.Config, error) {
	config, err := clientcmd.LoadFromFile(kubeconfigFile)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to load Kubeconfig file from %q", kubeconfigFile)
	}
	restConfig, err := clientcmd.NewDefaultClientConfig(*config, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		if strings.HasPrefix(err.Error(), "invalid configuration:") {
			return nil, errors.New(strings.Replace(err.Error(), "invalid configuration:", "invalid kubeconfig file; clusterctl requires a valid kubeconfig file to connect to the management cluster:", 1))
		}
		return nil, err
	}
	return restConfig, nil
}

func (k *proxy) getConfig() (*rest.Config, error) {
	var restConfig *restclient.Config

	// Check if kubeconfig is present and get restConfig. If not present, check if incluster restConfig can be got.
	restConfig, err := getRestConfig(k.kubeconfig)
	if err != nil {
		restConfig, err = rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("Failed inclusterConfig for kubeconfig with error: %v", err)
		}
	}

	restConfig.UserAgent = fmt.Sprintf("clusterctl/%s (%s)", version.Get().GitVersion, version.Get().Platform)

	// Set QPS and Burst to a threshold that ensures the controller runtime client/client go does't generate throttling log messages
	restConfig.QPS = 20
	restConfig.Burst = 100

	return restConfig, nil
}

func (k *proxy) newClientSet() (*kubernetes.Clientset, error) {
	config, err := k.getConfig()
	if err != nil {
		return nil, err
	}

	var cs *kubernetes.Clientset
	// Nb. The operation is wrapped in a retry loop to make newClientSet more resilient to temporary connection problems.
	connectBackoff := newConnectBackoff()
	if err := retryWithExponentialBackoff(connectBackoff, func() error {
		var err error
		cs, err = kubernetes.NewForConfig(config)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "failed to create the client-go client")
	}

	return cs, nil
}
