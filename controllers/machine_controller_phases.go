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
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/pointer"
	clusterv1 "sigs.k8s.io/cluster-api/api/v1alpha3"
	"sigs.k8s.io/cluster-api/controllers/external"
	"sigs.k8s.io/cluster-api/controllers/remote"
	cacpkv1 "sigs.k8s.io/cluster-api/controlplane/kubeadm/api/v1alpha3"
	capierrors "sigs.k8s.io/cluster-api/errors"
	"sigs.k8s.io/cluster-api/util"
	"sigs.k8s.io/cluster-api/util/annotations"
	"sigs.k8s.io/cluster-api/util/conditions"
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

func GetClusterResources(cluster *clusterv1.Cluster, c client.Client, scheme *runtime.Scheme, exclude_machine_name string) (string, error) {
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
		if exclude_machine_name == node.Name {
			continue
		}
		/* // If we need to exclude control plane nodes from cluster capacity, use this
		var is_control_plane bool = false
		for _, taint := range node.Spec.Taints {
			if taint.Key == "node-role.kubernetes.io/master" {
				is_control_plane = true
				break
			}
		}
		if is_control_plane {
			continue
		}*/
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
			clusterCMemoryResource = *resource.NewQuantity(0, resource.DecimalSI)
		}
		clusterCGPUResource, ok := clusterCResourceMap[ResourceGPU]
		if !ok {
			clusterCGPUResource = *resource.NewQuantity(0, resource.DecimalSI)
		}
		(&clusterCCPUResource).Set((&clusterCCPUResource).Value() + (&nodeCCPUResource).Value())
		(&clusterCGPUResource).Set((&clusterCGPUResource).Value() + (&nodeCGPUResource).Value())
		(&clusterCMemoryResource).Set((&clusterCMemoryResource).Value() + (&nodeCMemoryResource).Value())
		clusterCResourceMap[corev1.ResourceCPU] = clusterCCPUResource
		clusterCResourceMap[corev1.ResourceMemory] = clusterCMemoryResource
		clusterCResourceMap[ResourceGPU] = clusterCGPUResource
	}

	capacity, err := json.Marshal(clusterCResourceMap)
	if err != nil {
		return "", err
	}

	return string(capacity), nil
}

// Check if the cluster control planes are in running state
func (r *MachineReconciler) isControlPlanesRunning(cluster *clusterv1.Cluster, curMachine *clusterv1.Machine) bool {
	logger := r.Log.WithValues("cluster", cluster.Name, "namespace", cluster.Namespace)
	listOptions := []client.ListOption{
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels(map[string]string{clusterv1.ClusterLabelName: cluster.Name}),
	}

	var machines clusterv1.MachineList
	isrunning := false
	if err := r.Client.List(context.TODO(), &machines, listOptions...); err != nil {
		logger.Error(err, "failed to list Machines for cluster %s/%s", cluster.Namespace, cluster.Name)
		return isrunning
	}

	var provider string
	var ok bool
	if provider, ok = cluster.GetLabels()[KLabelProvider]; !ok {
		logger.Info("failed to get provider label for cluster")
		return isrunning
	}
	if provider == "azure" {
		name := fmt.Sprintf("%s-control-plane", cluster.Name)
		kcp := &cacpkv1.KubeadmControlPlane{}
		if err := r.Client.Get(context.TODO(), types.NamespacedName{Name: name, Namespace: cluster.Namespace}, kcp); err != nil {
			logger.Error(err, "failed to get KCP for cluster %s/%s", cluster.Namespace, cluster.Name)
			return isrunning
		}
		if kcp.Spec.Replicas != nil {
			if *kcp.Spec.Replicas != kcp.Status.Replicas {
				return isrunning
			}
		}
	}
	for _, machine := range machines.Items {
		if curMachine.Name == machine.Name {
			continue
		}
		// Exclude non-control-plane machines
		if _, ok := machine.GetLabels()[clusterv1.MachineControlPlaneLabelName]; !ok {
			continue
		}
		if machine.Status.Phase != "Running" {
			return isrunning
		}
	}

	return true
}

func (r *MachineReconciler) reconcilePhase(_ context.Context, m *clusterv1.Machine, c client.Client, cluster *clusterv1.Cluster) {
	logger := r.Log.WithValues("machine", m.Name, "namespace", m.Namespace)
	originalPhase := m.Status.Phase

	// Set the phase to "pending" if nil.
	if m.Status.Phase == "" {
		m.Status.SetTypedPhase(clusterv1.MachinePhasePending)
	}

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
		updateCluster := false
		patchHelper, err := patch.NewHelper(cluster, r.Client)
		if err != nil {
			logger.Error(err, "failed to create patch helper for cluster %s", cluster.Name)
		}
		now := metav1.Now()
		m.Status.LastUpdated = &now
		newPhase := m.Status.GetTypedPhase()
		if util.IsControlPlaneMachine(m) {
			updateCluster = true
			if newPhase == clusterv1.MachinePhaseRunning {
				if r.isControlPlanesRunning(cluster, m) {
					setAnnotation(cluster, KLabelClusterRunning, K8SProvisioned)
				}
			} else if newPhase == clusterv1.MachinePhaseDeleted || newPhase == clusterv1.MachinePhaseDeleting {
				if !r.isControlPlanesRunning(cluster, m) {
					setAnnotation(cluster, KLabelClusterRunning, K8SNotProvisioned)
				} else {
					setAnnotation(cluster, KLabelClusterRunning, K8SProvisioned)
				}
			} else {
				setAnnotation(cluster, KLabelClusterRunning, K8SNotProvisioned)
			}

		}

		if m.Spec.InfrastructureRef.Kind != "DiamantiMachine" && (newPhase == clusterv1.MachinePhaseRunning || newPhase == clusterv1.MachinePhaseFailed || newPhase == clusterv1.MachinePhaseDeleting) {
			exclude_machine_name := ""
			if newPhase != clusterv1.MachinePhaseRunning {
				exclude_machine_name = m.Name
			}
			capacity, err := GetClusterResources(cluster, r.Client, r.scheme, exclude_machine_name)
			if err != nil {
				logger.Error(err, "failed to get cluster resources")
			} else {
				setAnnotation(cluster, capacityResAnnotation, capacity)
				updateCluster = true
			}
		}
		if updateCluster && patchHelper != nil {
			if err := patchHelper.Patch(context.TODO(), cluster); err != nil {
				logger.Error(err, fmt.Sprintf("failed to set annotation for cluster %q/%q err %v", cluster.Namespace, cluster.Name, err))
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
	if annotations.IsPaused(cluster, obj) {
		logger.V(3).Info("External object referenced is paused")
		return external.ReconcileOutput{Paused: true}, nil
	}

	// Initialize the patch helper.
	patchHelper, err := patch.NewHelper(obj, r.Client)
	if err != nil {
		return external.ReconcileOutput{}, err
	}

	// With the migration from v1alpha2 to v1alpha3, Machine controllers should be the owner for the
	// infra Machines, hence remove any existing machineset controller owner reference
	if controller := metav1.GetControllerOf(obj); controller != nil && controller.Kind == "MachineSet" {
		gv, err := schema.ParseGroupVersion(controller.APIVersion)
		if err != nil {
			return external.ReconcileOutput{}, err
		}
		if gv.Group == clusterv1.GroupVersion.Group {
			ownerRefs := util.RemoveOwnerRef(obj.GetOwnerReferences(), *controller)
			obj.SetOwnerReferences(ownerRefs)
		}
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
	// If the bootstrap data is populated, set ready and return.
	if m.Spec.Bootstrap.DataSecretName != nil {
		m.Status.BootstrapReady = true
		conditions.MarkTrue(m, clusterv1.BootstrapReadyCondition)
		return nil
	}

	// If the Boostrap ref is nil (and so the machine should use user generated data secret), return.
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

	// If the bootstrap config is being deleted, return early.
	if !bootstrapConfig.GetDeletionTimestamp().IsZero() {
		return nil
	}

	// Determine if the bootstrap provider is ready.
	ready, err := external.IsReady(bootstrapConfig)
	if err != nil {
		return err
	}

	// Report a summary of current status of the bootstrap object defined for this machine.
	conditions.SetMirror(m, clusterv1.BootstrapReadyCondition,
		conditions.UnstructuredGetter(bootstrapConfig),
		conditions.WithFallbackValue(ready, clusterv1.WaitingForDataSecretFallbackReason, clusterv1.ConditionSeverityInfo, ""),
	)

	// If the bootstrap provider is not ready, requeue.
	if !ready {
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

	// Report a summary of current status of the infrastructure object defined for this machine.
	conditions.SetMirror(m, clusterv1.InfrastructureReadyCondition,
		conditions.UnstructuredGetter(infraConfig),
		conditions.WithFallbackValue(ready, clusterv1.WaitingForInfrastructureFallbackReason, clusterv1.ConditionSeverityInfo, ""),
	)

	// If the infrastructure provider is not ready, return early.
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
