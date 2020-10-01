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

package controllers

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/pointer"
	clusterv1 "sigs.k8s.io/cluster-api/api/v1alpha3"
	"sigs.k8s.io/cluster-api/controllers/external"
	capierrors "sigs.k8s.io/cluster-api/errors"
	"sigs.k8s.io/cluster-api/util"
	utilconversion "sigs.k8s.io/cluster-api/util/conversion"
	"sigs.k8s.io/cluster-api/util/kubeconfig"
	"sigs.k8s.io/cluster-api/util/patch"
	"sigs.k8s.io/cluster-api/util/secret"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
)

const deploymentName = "enforcer-prod"
const deploymentNamespace = "kube-system"

// Check if the cluster has atleast one control plane in running state
func isClusterRunning(cluster *clusterv1.Cluster) bool {
	annotations := cluster.ObjectMeta.GetAnnotations()
	if annotations != nil && annotations[KLabelClusterRunning] == K8SProvisioned {
		return true
	}
	return false
}

func (r *ClusterReconciler) isTenantClusterReady(cluster *clusterv1.Cluster) bool {
	logger := r.Log.WithValues("cluster", cluster.Name, "namespace", cluster.Namespace)

	obj := &corev1.Secret{}
	secretName := fmt.Sprintf("%s-kubeconfig", cluster.Name)
	err := r.Client.Get(context.TODO(),
		types.NamespacedName{Name: secretName, Namespace: cluster.Namespace}, obj)
	if err != nil {
		return false
	}
	token, ok := obj.Data["value"]
	if !ok {
		token, ok = obj.Data["config"]
		if !ok {
			logger.Info(fmt.Sprintf("Cluster %s-kubeconfig secret not found", cluster.Name))
			return false
		}
	}
	dir := os.TempDir()
	tmpFile, err := ioutil.TempFile(dir, "tkubeconfig-")
	if err != nil {
		logger.Info(fmt.Sprintf("tempfile create failed for cluster %s status: %v ", cluster.Name, err))
		return false
	}

	// Remember to clean up the file afterwards
	defer os.Remove(dir)

	err = ioutil.WriteFile(tmpFile.Name(), token, 0644)
	if err != nil {
		logger.Info(fmt.Sprintf("tempfile write failed for cluster %s,status: %v ", err))
		return false
	}

	config, err := clientcmd.BuildConfigFromFlags("", tmpFile.Name())
	if err != nil {
		logger.Info(fmt.Sprintf("Cluster %s clientcmd config not found", cluster.Name))
		return false
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		logger.Info(fmt.Sprintf("Cluster %s clientset not found", cluster.Name))
		return false
	}

	deploymentsClient := clientset.AppsV1().Deployments(deploymentNamespace)

	deployment, err := deploymentsClient.Get(deploymentName, metav1.GetOptions{})

	if err == nil && deployment.Status.AvailableReplicas > 0 {
		logger.Info("Cluster initialized by tenant controller, moving to Running phase")
		return true
	}
	return false
}

func (r *ClusterReconciler) reconcilePhase(_ context.Context, cluster *clusterv1.Cluster) error {
	if cluster.Status.Phase == "" {
		cluster.Status.SetTypedPhase(clusterv1.ClusterPhasePending)
	}

	if cluster.Spec.InfrastructureRef != nil {
		cluster.Status.SetTypedPhase(clusterv1.ClusterPhaseProvisioning)
	}

	if cluster.Status.InfrastructureReady && (isClusterExternallyProvisioned(cluster) || !cluster.Spec.ControlPlaneEndpoint.IsZero()) {
		cluster.Status.SetTypedPhase(clusterv1.ClusterPhaseProvisioned)
	}

	if isClusterRunning(cluster) {
		if r.isTenantClusterReady(cluster) == false {
			return errors.New("Cluster still not ready to be used")
		}
		cluster.Status.SetTypedPhase(clusterv1.ClusterPhaseRunning)
	}

	if cluster.Status.FailureReason != nil || cluster.Status.FailureMessage != nil {
		cluster.Status.SetTypedPhase(clusterv1.ClusterPhaseFailed)
	}

	if !cluster.DeletionTimestamp.IsZero() {
		cluster.Status.SetTypedPhase(clusterv1.ClusterPhaseDeleting)
	}
	return nil
}

// reconcileExternal handles generic unstructured objects referenced by a Cluster.
func (r *ClusterReconciler) reconcileExternal(ctx context.Context, cluster *clusterv1.Cluster, ref *corev1.ObjectReference) (external.ReconcileOutput, error) {
	logger := r.Log.WithValues("cluster", cluster.Name, "namespace", cluster.Namespace)

	if err := utilconversion.ConvertReferenceAPIContract(ctx, r.Client, ref); err != nil {
		return external.ReconcileOutput{}, err
	}

	obj, err := external.Get(ctx, r.Client, ref, cluster.Namespace)
	if err != nil {
		if apierrors.IsNotFound(errors.Cause(err)) {
			return external.ReconcileOutput{}, errors.Wrapf(&capierrors.RequeueAfterError{RequeueAfter: 30 * time.Second},
				"could not find %v %q for Cluster %q in namespace %q, requeuing",
				ref.GroupVersionKind(), ref.Name, cluster.Name, cluster.Namespace)
		}
		return external.ReconcileOutput{}, err
	}

	// if external ref is paused, return error.
	if util.IsPaused(cluster, obj) {
		logger.V(3).Info("External object referenced is paused")
		return external.ReconcileOutput{Paused: true}, nil
	}

	// Initialize the patch helper.
	patchHelper, err := patch.NewHelper(obj, r.Client)
	if err != nil {
		return external.ReconcileOutput{}, err
	}

	// Set external object ControllerReference to the Cluster.
	if err := controllerutil.SetControllerReference(cluster, obj, r.scheme); err != nil {
		return external.ReconcileOutput{}, err
	}

	// Set the Cluster label.
	labels := obj.GetLabels()
	if labels == nil {
		labels = make(map[string]string)
	}
	labels[clusterv1.ClusterLabelName] = cluster.Name
	obj.SetLabels(labels)

	// Always attempt to Patch the external object.
	if err := patchHelper.Patch(ctx, obj); err != nil {
		return external.ReconcileOutput{}, err
	}

	// Ensure we add a watcher to the external object.
	if err := r.externalTracker.Watch(logger, obj, &handler.EnqueueRequestForOwner{OwnerType: &clusterv1.Cluster{}}); err != nil {
		return external.ReconcileOutput{}, err
	}

	// Set failure reason and message, if any.
	failureReason, failureMessage, err := external.FailuresFrom(obj)
	if err != nil {
		return external.ReconcileOutput{}, err
	}
	if failureReason != "" {
		clusterStatusError := capierrors.ClusterStatusError(failureReason)
		cluster.Status.FailureReason = &clusterStatusError
	}
	if failureMessage != "" {
		cluster.Status.FailureMessage = pointer.StringPtr(
			fmt.Sprintf("Failure detected from referenced resource %v with name %q: %s",
				obj.GroupVersionKind(), obj.GetName(), failureMessage),
		)
	}

	return external.ReconcileOutput{Result: obj}, nil
}

// reconcileInfrastructure reconciles the Spec.InfrastructureRef object on a Cluster.
func (r *ClusterReconciler) reconcileInfrastructure(ctx context.Context, cluster *clusterv1.Cluster) error {
	logger := r.Log.WithValues("cluster", cluster.Name, "namespace", cluster.Namespace)
	if isClusterExternallyProvisioned(cluster) {
		cluster.Status.InfrastructureReady = true
		setAnnotation(cluster, KLabelClusterRunning, K8SProvisioned)
		capacity, err := GetClusterResources(cluster, r.Client, r.scheme, "")
		if err != nil {
			logger.V(3).Info("Failed to get cluster resources with error: %v", err)
			return err
		}
		//setAnnotation(cluster, allocResAnnotation, alloc)
		setAnnotation(cluster, capacityResAnnotation, capacity)
		return nil
	}

	if cluster.Spec.InfrastructureRef == nil {
		return nil
	}

	// Call generic external reconciler.
	infraReconcileResult, err := r.reconcileExternal(ctx, cluster, cluster.Spec.InfrastructureRef)
	if err != nil {
		return err
	}
	// if the external object is paused, return without any further processing
	if infraReconcileResult.Paused {
		return nil
	}
	infraConfig := infraReconcileResult.Result

	// There's no need to go any further if the Cluster is marked for deletion.
	if !infraConfig.GetDeletionTimestamp().IsZero() {
		return nil
	}

	// Determine if the infrastructure provider is ready.
	ready, err := external.IsReady(infraConfig)
	if err != nil {
		return err
	}
	cluster.Status.InfrastructureReady = ready
	if !ready {
		logger.V(3).Info("Infrastructure provider is not ready yet")
		return nil
	}

	// Get and parse Spec.ControlPlaneEndpoint field from the infrastructure provider.
	if cluster.Spec.ControlPlaneEndpoint.IsZero() {
		if err := util.UnstructuredUnmarshalField(infraConfig, &cluster.Spec.ControlPlaneEndpoint, "spec", "controlPlaneEndpoint"); err != nil {
			return errors.Wrapf(err, "failed to retrieve Spec.ControlPlaneEndpoint from infrastructure provider for Cluster %q in namespace %q",
				cluster.Name, cluster.Namespace)
		}
	}

	// Get and parse Status.FailureDomains from the infrastructure provider.
	if err := util.UnstructuredUnmarshalField(infraConfig, &cluster.Status.FailureDomains, "status", "failureDomains"); err != nil && err != util.ErrUnstructuredFieldNotFound {
		return errors.Wrapf(err, "failed to retrieve Status.FailureDomains from infrastructure provider for Cluster %q in namespace %q",
			cluster.Name, cluster.Namespace)
	}

	return nil
}

// reconcileControlPlane reconciles the Spec.ControlPlaneRef object on a Cluster.
func (r *ClusterReconciler) reconcileControlPlane(ctx context.Context, cluster *clusterv1.Cluster) error {
	if isClusterExternallyProvisioned(cluster) {
		cluster.Status.InfrastructureReady = true
		return nil
	}

	if cluster.Spec.ControlPlaneRef == nil {
		return nil
	}

	// Call generic external reconciler.
	controlPlaneReconcileResult, err := r.reconcileExternal(ctx, cluster, cluster.Spec.ControlPlaneRef)
	if err != nil {
		return err
	}
	// if the external object is paused, return without any further processing
	if controlPlaneReconcileResult.Paused {
		return nil
	}
	controlPlaneConfig := controlPlaneReconcileResult.Result

	// There's no need to go any further if the control plane resource is marked for deletion.
	if !controlPlaneConfig.GetDeletionTimestamp().IsZero() {
		return nil
	}

	// Update cluster.Status.ControlPlaneInitialized if it hasn't already been set
	// Determine if the control plane provider is initialized.
	if !cluster.Status.ControlPlaneInitialized {
		initialized, err := external.IsInitialized(controlPlaneConfig)
		if err != nil {
			return err
		}
		cluster.Status.ControlPlaneInitialized = initialized
	}

	// Determine if the control plane provider is ready.
	ready, err := external.IsReady(controlPlaneConfig)
	if err != nil {
		return err
	}
	cluster.Status.ControlPlaneReady = ready

	return nil
}

func (r *ClusterReconciler) reconcileKubeconfig(ctx context.Context, cluster *clusterv1.Cluster) error {
	if cluster.Spec.ControlPlaneEndpoint.IsZero() && !isClusterExternallyProvisioned(cluster) {
		return nil
	}

	// Do not generate the Kubeconfig if there is a ControlPlaneRef, since the Control Plane provider is
	// responsible for the management of the Kubeconfig. We continue to manage it here only for backward
	// compatibility when a Control Plane provider is not in use.
	if cluster.Spec.ControlPlaneRef != nil {
		return nil
	}

	s, err := secret.Get(ctx, r.Client, util.ObjectKey(cluster), secret.Kubeconfig)
	switch {
	case apierrors.IsNotFound(err):
		if err := kubeconfig.CreateSecret(ctx, r.Client, cluster); err != nil {
			if err == kubeconfig.ErrDependentCertificateNotFound {
				return errors.Wrapf(&capierrors.RequeueAfterError{RequeueAfter: 30 * time.Second},
					"could not find secret %q for Cluster %q in namespace %q, requeuing",
					secret.ClusterCA, cluster.Name, cluster.Namespace)
			}
			return err
		}
	case err != nil:
		return errors.Wrapf(err, "failed to retrieve Kubeconfig Secret for Cluster %q in namespace %q", cluster.Name, cluster.Namespace)
	}

	// Make sure the ownerReference is set for the secret
	if !util.HasOwnerRef(s.GetOwnerReferences(), metav1.OwnerReference{
		APIVersion: clusterv1.GroupVersion.String(),
		Kind:       "Cluster",
		Name:       cluster.Name,
		UID:        cluster.UID,
	}) {
		s.SetOwnerReferences(util.EnsureOwnerRef(s.GetOwnerReferences(), metav1.OwnerReference{
			APIVersion: clusterv1.GroupVersion.String(),
			Kind:       "Cluster",
			Name:       cluster.Name,
			UID:        cluster.UID,
		}))
		err := r.Client.Update(context.TODO(), s)
		if err != nil {
			return errors.Wrapf(err, "failed to update secret %s", s.Name)
		}
	}

	return nil
}
