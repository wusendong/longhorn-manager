package v1

import (
	"github.com/rancher/longhorn-manager/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	OperatorVersion = "v1"
	Group           = "longhorn.rancher.com"
)

// Describes an volume cluster.
type Volume struct {
	metav1.TypeMeta `json:",inline"`
	// Standard object’s metadata. More info:
	// https://github.com/kubernetes/community/blob/master/contributors/devel/api-conventions.md#metadata
	metav1.ObjectMeta `json:"metadata,omitempty"`
	// SpecificTypeMetaation of the desired behavior of the volume cluster. More info:
	// https://github.com/kubernetes/community/blob/master/contributors/devel/api-conventions.md#spec-and-status
	Spec VolumeSpec `json:"spec"`
	// Most recent observed status of the volume cluster. Read-only. Not
	// included when requesting from the apiserver, only from the Prometheus
	// Operator API itself. More info:
	// https://github.com/kubernetes/community/blob/master/contributors/devel/api-conventions.md#spec-and-status
	Status *VolumeStatus `json:"status,omitempty"`
}

// Specification of the desired behavior of the volume cluster. More info:
// https://github.com/kubernetes/community/blob/master/contributors/devel/api-conventions.md#spec-and-status
type VolumeSpec struct {
	// Standard object’s metadata. More info:
	// https://github.com/kubernetes/community/blob/master/contributors/devel/api-conventions.md#metadata
	// Metadata Labels and Annotations gets propagated to the prometheus pods.
	PodMetadata *metav1.ObjectMeta `json:"podMetadata,omitempty"`

	Volume *types.VolumeSpecInfo
}

// A list of volumes.
type VolumeList struct {
	metav1.TypeMeta `json:",inline"`
	// Standard list metadata
	// More info: https://github.com/kubernetes/community/blob/master/contributors/devel/api-conventions.md#metadata
	metav1.ListMeta `json:"metadata,omitempty"`
	// List of volumes
	Items []Volume `json:"items"`
}

// Most recent observed status of the volume cluster. Read-only. Not
// included when requesting from the apiserver, only from the Prometheus
// Operator API itself. More info:
// https://github.com/kubernetes/community/blob/master/contributors/devel/api-conventions.md#spec-and-status
type VolumeStatus struct {
	Volume     *types.VolumeRunningInfo
	Controller *types.ControllerInfo
	Replicas   []*types.ReplicaInfo
}

// Describes an setting.
type Setting struct {
	metav1.TypeMeta `json:",inline"`
	// Standard object’s metadata. More info:
	// https://github.com/kubernetes/community/blob/master/contributors/devel/api-conventions.md#metadata
	metav1.ObjectMeta `json:"metadata,omitempty"`
	// SpecificTypeMetaation of the desired behavior of the setting cluster. More info:
	// https://github.com/kubernetes/community/blob/master/contributors/devel/api-conventions.md#spec-and-status
	Spec SettingSpec `json:"spec"`
	// Most recent observed status of the setting cluster. Read-only. Not
	// included when requesting from the apiserver, only from the Prometheus
	// Operator API itself. More info:
	// https://github.com/kubernetes/community/blob/master/contributors/devel/api-conventions.md#spec-and-status
	Status *SettingStatus `json:"status,omitempty"`
}

// Specification of the desired behavior of the setting cluster. More info:
// https://github.com/kubernetes/community/blob/master/contributors/devel/api-conventions.md#spec-and-status
type SettingSpec struct {
	// Standard object’s metadata. More info:
	// https://github.com/kubernetes/community/blob/master/contributors/devel/api-conventions.md#metadata
	// Metadata Labels and Annotations gets propagated to the prometheus pods.
	PodMetadata *metav1.ObjectMeta `json:"podMetadata,omitempty"`
	// Version the cluster should be on.
	Version string `json:"version,omitempty"`
	// BackupTarget
	BackupTarget string `json:"backupTarget"`
}

// A list of settings.
type SettingList struct {
	metav1.TypeMeta `json:",inline"`
	// Standard list metadata
	// More info: https://github.com/kubernetes/community/blob/master/contributors/devel/api-conventions.md#metadata
	metav1.ListMeta `json:"metadata,omitempty"`
	// List of settings
	Items []Setting `json:"items"`
}

// Most recent observed status of the setting cluster. Read-only. Not
// included when requesting from the apiserver, only from the Prometheus
// Operator API itself. More info:
// https://github.com/kubernetes/community/blob/master/contributors/devel/api-conventions.md#spec-and-status
type SettingStatus struct {
	TargetNodeID string

	Created       string
	NodeID        string
	Endpoint      string
	RecurringJobs []types.RecurringJob

	Replicas []types.ReplicaInfo
}
