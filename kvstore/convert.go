package kvstore

import (
	"github.com/pkg/errors"
	lv1 "github.com/rancher/longhorn-manager/client/v1"
	"github.com/rancher/longhorn-manager/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func ToSetting(obj interface{}) (*lv1.Setting, error) {
	setting, ok := obj.(*types.SettingsInfo)
	if !ok {
		return nil, errors.Errorf("Mismatch type: %T", obj)
	}

	return &lv1.Setting{
		ObjectMeta: metav1.ObjectMeta{
			Name: keySettings,
		},
		Spec: lv1.SettingSpec{
			BackupTarget: setting.BackupTarget,
		},
	}, nil
}
func ToVolume(obj interface{}) (*lv1.Volume, error) {
	volume, ok := obj.(*types.VolumeInfo)
	if !ok {
		return nil, errors.Errorf("Mismatch type: %T", obj)
	}

	return &lv1.Volume{
		ObjectMeta: metav1.ObjectMeta{
			Name: volume.Name,
		},
		Spec: lv1.VolumeSpec{
			Volume: &volume.VolumeSpecInfo,
		},
		Status: &lv1.VolumeStatus{
			Volume: &volume.VolumeRunningInfo,
		},
	}, nil
}
