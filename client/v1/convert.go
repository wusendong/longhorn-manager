package v1

import (
	"github.com/pkg/errors"
	"github.com/rancher/longhorn-manager/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func ToSetting(obj interface{}) (*Setting, error) {
	setting, ok := obj.(*types.SettingsInfo)
	if !ok {
		return nil, errors.Errorf("Mismatch type: %T", obj)
	}

	return &Setting{
		ObjectMeta: metav1.ObjectMeta{
			Name: "setting",
		},
		Spec: SettingSpec{
			BackupTarget: setting.BackupTarget,
		},
	}, nil
}
