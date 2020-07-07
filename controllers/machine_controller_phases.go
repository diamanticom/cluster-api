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
	"strings"
	"time"

	"encoding/json"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/pointer"
	//"log"
	clusterv1 "sigs.k8s.io/cluster-api/api/v1alpha3"
	"sigs.k8s.io/cluster-api/controllers/external"
	"sigs.k8s.io/cluster-api/controllers/remote"
	capierrors "sigs.k8s.io/cluster-api/errors"
	"sigs.k8s.io/cluster-api/util"
	utilconversion "sigs.k8s.io/cluster-api/util/conversion"
	"sigs.k8s.io/cluster-api/util/patch"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
)

var (
	externalReadyWait = 30 * time.Second
)

const (
	ResourceGPU corev1.ResourceName = "nvidia.com/gpu"
)

func setAnnotation(cluster *clusterv1.Cluster, annotation string, value string) {
	annotations := cluster.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[annotation] = value
	cluster.SetAnnotations(annotations)
}

func GetClusterResources(cluster *clusterv1.Cluster, c client.Client, scheme *runtime.Scheme) (string, error) {
	restConfig, err := remote.RESTConfig(context.TODO(), c, util.ObjectKey(cluster))
	if err != nil {
		return "", err
	}

	remoteClient, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return "", err
	}

	clusterCResourceMap := make(corev1.ResourceList)
	nodes, err := remoteClient.CoreV1().Nodes().List(metav1.ListOptions{})
	for _, node := range nodes.Items {
		//klog.Infof("Node %s allocatable %v", node.Name, AresourceList)
		//klog.Infof("clusterCPUResource  %+v", clusterCPUResource)
		//klog.Infof("clusterMemoryResource %+v", clusterMemoryResource)

		CresourceList := node.Status.Capacity
		nodeCCPUResource := CresourceList[corev1.ResourceCPU]
		nodeCMemoryResource := CresourceList[corev1.ResourceMemory]
		nodeCGPUResource := CresourceList[ResourceGPU]
		clusterCCPUResource, ok := clusterCResourceMap[corev1.ResourceCPU]
		if !ok {
			clusterCCPUResource = *resource.NewQuantity(0, resource.DecimalSI)
		}
		clusterCMemoryResource, ok := clusterCResourceMap[corev1.ResourceMemory]
		if !ok {
			clusterCMemoryResource = *resource.NewQuantity(0, resource.BinarySI)
		}
		clusterCGPUResource, ok := clusterCResourceMap[ResourceGPU]
		if !ok {
			clusterCGPUResource = *resource.NewQuantity(0, resource.DecimalSI)
		}
		(&clusterCCPUResource).Set((&clusterCCPUResource).Value() + (&nodeCCPUResource).Value())
		(&clusterCGPUResource).Set((&clusterCGPUResource).Value() + (&nodeCGPUResource).Value())
		(&clusterCMemoryResource).Set((&clusterCMemoryResource).Value() + ((&nodeCMemoryResource).Value())/(1024*1024*1024))

		//log.Printf("Node %s is %+v, %+v", node.Name, nodeCMemoryResource, clusterCMemoryResource)
		//klog.Infof("clusterCPUResource  %+v", clusterCPUResource)
		//klog.Infof("clusterMemoryResource %+v", clusterMemoryResource)
		clusterCResourceMap[corev1.ResourceCPU] = clusterCCPUResource
		clusterCResourceMap[corev1.ResourceMemory] = clusterCMemoryResource
		clusterCResourceMap[ResourceGPU] = clusterCGPUResource
	}

	//klog.Infof("clusterResourceMap  %+v", clusterResourceMap)
	capacity, err := json.Marshal(clusterCResourceMap)
	if err != nil {
		return "", err
	}

	return string(capacity), nil
}

func (r *MachineReconciler) reconcilePhase(_ context.Context, m *clusterv1.Machine, c client.Client, cluster *clusterv1.Cluster) {
	originalPhase := m.Status.Phase

	// Set the phase to "pending" if nil.
	if m.Status.Phase == "" {
		m.Status.SetTypedPhase(clusterv1.MachinePhasePending)
	}

	oldPhase := m.Status.GetTypedPhase()
	// Set the phase to "provisioning" if bootstrap is ready and the infrastructure isn't.
	if m.Status.BootstrapReady && !m.Status.InfrastructureReady {
		m.Status.SetTypedPhase(clusterv1.MachinePhaseProvisioning)
	}

	// Set the phase to "provisioned" if there is a NodeRef.
	if m.Status.NodeRef != nil {
		m.Status.SetTypedPhase(clusterv1.MachinePhaseProvisioned)
	}

	// Set the phase to "running" if there is a NodeRef field and infrastructure is ready.
	if m.Status.NodeRef != nil && m.Status.InfrastructureReady {
		m.Status.SetTypedPhase(clusterv1.MachinePhaseRunning)
	}

	// Set the phase to "failed" if any of Status.FailureReason or Status.FailureMessage is not-nil.
	if m.Status.FailureReason != nil || m.Status.FailureMessage != nil {
		m.Status.SetTypedPhase(clusterv1.MachinePhaseFailed)
	}

	// Set the phase to "deleting" if the deletion timestamp is set.
	if !m.DeletionTimestamp.IsZero() {
		m.Status.SetTypedPhase(clusterv1.MachinePhaseDeleting)
	}

	// If the phase has changed, update the LastUpdated timestamp
	if m.Status.Phase != originalPhase {
		now := metav1.Now()
		m.Status.LastUpdated = &now
		newPhase := m.Status.GetTypedPhase()
		if util.IsControlPlaneMachine(m) && newPhase == clusterv1.MachinePhaseRunning && oldPhase != clusterv1.MachinePhaseRunning && cluster != nil {
			setAnnotation(cluster, KLabelClusterRunning, K8SProvisioned)
			if err := c.Update(context.TODO(), cluster); err != nil {
				r.Log.Info(fmt.Sprintf("failed to set annotation for cluster %q/%q", cluster.Namespace, cluster.Name))
			}
		}
		if newPhase == clusterv1.MachinePhaseRunning || newPhase == clusterv1.MachinePhaseFailed || newPhase == clusterv1.MachinePhaseDeleting {
			capacity, err := GetClusterResources(cluster, r.Client, r.scheme)
			if err != nil {
				r.Log.Info(fmt.Sprintf("Failed to get cluster resources with error: %v", err))
			}
			setAnnotation(cluster, capacityResAnnotation, capacity)
			if err := c.Update(context.TODO(), cluster); err != nil {
				r.Log.Info(fmt.Sprintf("failed to set annotation for cluster %q/%q", cluster.Namespace, cluster.Name))
			}
		}
	}
}

// reconcileExternal handles generic unstructured objects referenced by a Machine.
func (r *MachineReconciler) reconcileExternal(ctx context.Context, cluster *clusterv1.Cluster, m *clusterv1.Machine, ref *corev1.ObjectReference) (external.ReconcileOutput, error) {
	logger := r.Log.WithValues("machine", m.Name, "namespace", m.Namespace)

	if err := utilconversion.ConvertReferenceAPIContract(ctx, r.Client, ref); err != nil {
		return external.ReconcileOutput{}, err
	}

	obj, err := external.Get(ctx, r.Client, ref, m.Namespace)
	if err != nil {
		if apierrors.IsNotFound(errors.Cause(err)) {
			return external.ReconcileOutput{}, errors.Wrapf(&capierrors.RequeueAfterError{RequeueAfter: externalReadyWait},
				"could not find %v %q for Machine %q in namespace %q, requeuing",
				ref.GroupVersionKind(), ref.Name, m.Name, m.Namespace)
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

	// Set external object ControllerReference to the Machine.
	if err := controllerutil.SetControllerReference(m, obj, r.scheme); err != nil {
		return external.ReconcileOutput{}, err
	}

	// Set the Cluster label.
	labels := obj.GetLabels()
	if labels == nil {
		labels = make(map[string]string)
	}
	labels[clusterv1.ClusterLabelName] = m.Spec.ClusterName
	obj.SetLabels(labels)

	// Always attempt to Patch the external object.
	if err := patchHelper.Patch(ctx, obj); err != nil {
		return external.ReconcileOutput{}, err
	}

	// Ensure we add a watcher to the external object.
	if err := r.externalTracker.Watch(logger, obj, &handler.EnqueueRequestForOwner{OwnerType: &clusterv1.Machine{}}); err != nil {
		return external.ReconcileOutput{}, err
	}

	// Set failure reason and message, if any.
	failureReason, failureMessage, err := external.FailuresFrom(obj)
	if err != nil {
		return external.ReconcileOutput{}, err
	}
	if failureReason != "" {
		machineStatusError := capierrors.MachineStatusError(failureReason)
		m.Status.FailureReason = &machineStatusError
	}
	if failureMessage != "" {
		m.Status.FailureMessage = pointer.StringPtr(
			fmt.Sprintf("Failure detected from referenced resource %v with name %q: %s",
				obj.GroupVersionKind(), obj.GetName(), failureMessage),
		)
	}

	return external.ReconcileOutput{Result: obj}, nil
}

// reconcileBootstrap reconciles the Spec.Bootstrap.ConfigRef object on a Machine.
func (r *MachineReconciler) reconcileBootstrap(ctx context.Context, cluster *clusterv1.Cluster, m *clusterv1.Machine) error {
	if m.Spec.Bootstrap.ConfigRef == nil {
		return nil
	}

	// Call generic external reconciler if we have an external reference.
	externalResult, err := r.reconcileExternal(ctx, cluster, m, m.Spec.Bootstrap.ConfigRef)
	if err != nil {
		return err
	}
	if externalResult.Paused {
		return nil
	}
	bootstrapConfig := externalResult.Result

	// If the bootstrap data is populated, set ready and return.
	if m.Spec.Bootstrap.DataSecretName != nil {
		m.Status.BootstrapReady = true
		return nil
	}

	// If the bootstrap config is being deleted, return early.
	if !bootstrapConfig.GetDeletionTimestamp().IsZero() {
		return nil
	}

	// Determine if the bootstrap provider is ready.
	ready, err := external.IsReady(bootstrapConfig)
	if err != nil {
		return err
	} else if !ready {
		return errors.Wrapf(&capierrors.RequeueAfterError{RequeueAfter: externalReadyWait},
			"Bootstrap provider for Machine %q in namespace %q is not ready, requeuing", m.Name, m.Namespace)
	}

	// Get and set the name of the secret containing the bootstrap data.
	secretName, _, err := unstructured.NestedString(bootstrapConfig.Object, "status", "dataSecretName")
	if err != nil {
		return errors.Wrapf(err, "failed to retrieve dataSecretName from bootstrap provider for Machine %q in namespace %q", m.Name, m.Namespace)
	} else if secretName == "" {
		return errors.Errorf("retrieved empty dataSecretName from bootstrap provider for Machine %q in namespace %q", m.Name, m.Namespace)
	}

	m.Spec.Bootstrap.Data = nil
	m.Spec.Bootstrap.DataSecretName = pointer.StringPtr(secretName)
	m.Status.BootstrapReady = true
	return nil
}

// reconcileInfrastructure reconciles the Spec.InfrastructureRef object on a Machine.
func (r *MachineReconciler) reconcileInfrastructure(ctx context.Context, cluster *clusterv1.Cluster, m *clusterv1.Machine) error {
	// Call generic external reconciler.
	infraReconcileResult, err := r.reconcileExternal(ctx, cluster, m, &m.Spec.InfrastructureRef)
	if err != nil {
		if m.Status.InfrastructureReady && strings.Contains(err.Error(), "could not find") {
			// Infra object went missing after the machine was up and running
			r.Log.Error(err, "Machine infrastructure reference has been deleted after being ready, setting failure state")
			m.Status.FailureReason = capierrors.MachineStatusErrorPtr(capierrors.InvalidConfigurationMachineError)
			m.Status.FailureMessage = pointer.StringPtr(fmt.Sprintf("Machine infrastructure resource %v with name %q has been deleted after being ready",
				m.Spec.InfrastructureRef.GroupVersionKind(), m.Spec.InfrastructureRef.Name))
		}
		return err
	}
	// if the external object is paused, return without any further processing
	if infraReconcileResult.Paused {
		return nil
	}
	infraConfig := infraReconcileResult.Result

	if !infraConfig.GetDeletionTimestamp().IsZero() {
		return nil
	}

	// Determine if the infrastructure provider is ready.
	ready, err := external.IsReady(infraConfig)
	if err != nil {
		return err
	}
	m.Status.InfrastructureReady = ready
	if !ready {
		return errors.Wrapf(&capierrors.RequeueAfterError{RequeueAfter: externalReadyWait},
			"Infrastructure provider for Machine %q in namespace %q is not ready, requeuing", m.Name, m.Namespace,
		)
	}
	if m.Spec.InfrastructureRef.Kind == "DiamantiMachine" {
		var x string = "diamanti"
		m.Spec.ProviderID = pointer.StringPtr(x)
		m.Status.InfrastructureReady = true
		return nil
	}

	// Get Spec.ProviderID from the infrastructure provider.
	var providerID string
	if err := util.UnstructuredUnmarshalField(infraConfig, &providerID, "spec", "providerID"); err != nil {
		return errors.Wrapf(err, "failed to retrieve Spec.ProviderID from infrastructure provider for Machine %q in namespace %q", m.Name, m.Namespace)
	} else if providerID == "" {
		return errors.Errorf("retrieved empty Spec.ProviderID from infrastructure provider for Machine %q in namespace %q", m.Name, m.Namespace)
	}

	// Get and set Status.Addresses from the infrastructure provider.
	err = util.UnstructuredUnmarshalField(infraConfig, &m.Status.Addresses, "status", "addresses")
	if err != nil && err != util.ErrUnstructuredFieldNotFound {
		return errors.Wrapf(err, "failed to retrieve addresses from infrastructure provider for Machine %q in namespace %q", m.Name, m.Namespace)
	}

	// Get and set the failure domain from the infrastructure provider.
	var failureDomain string
	err = util.UnstructuredUnmarshalField(infraConfig, &failureDomain, "spec", "failureDomain")
	switch {
	case err == util.ErrUnstructuredFieldNotFound: // no-op
	case err != nil:
		return errors.Wrapf(err, "failed to failure domain from infrastructure provider for Machine %q in namespace %q", m.Name, m.Namespace)
	default:
		m.Spec.FailureDomain = pointer.StringPtr(failureDomain)
	}

	m.Spec.ProviderID = pointer.StringPtr(providerID)
	return nil
}
