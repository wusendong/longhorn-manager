package kvstore

import (
	"path/filepath"
	"regexp"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/util"

	"github.com/pkg/errors"

	crdCli "github.com/rancher/longhorn-manager/client"
	lv1 "github.com/rancher/longhorn-manager/client/v1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	extensionsobj "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	apiv1 "k8s.io/client-go/pkg/api/v1"
)

type CRDBackend struct {
	crdclient *apiextensionsclient.Clientset
	lcli      *crdCli.Clientset
	kcli      *kubernetes.Clientset
	namespace string
	prefix    string

	vcli   lv1.VolumeInterface
	setcli lv1.SettingInterface
}

func NewCRDBackend(prefix, namespace string, cfg *rest.Config) (*CRDBackend, error) {
	crdclient, err := apiextensionsclient.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	kcli, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "cannot connect to k8s")
	}

	lcli, err := crdCli.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	backend := &CRDBackend{
		prefix:    prefix,
		crdclient: crdclient,
		lcli:      lcli,
		kcli:      kcli,
		namespace: namespace,
		vcli:      lcli.LonghornV1().Volumes(namespace),
		setcli:    lcli.LonghornV1().Settings(namespace),
	}
	if err := backend.ensureCRD(); err != nil {
		return nil, err
	}
	return backend, nil
}

func (s *CRDBackend) ensureCRD() error {
	listFuncs := []func(opts metav1.ListOptions) (runtime.Object, error){
		s.lcli.LonghornV1().Settings(s.namespace).List,
		s.lcli.LonghornV1().Volumes(s.namespace).List,
	}
	exists, err := util.CrdExists(listFuncs...)
	if err == nil && exists {
		return nil
	}

	crds := []*extensionsobj.CustomResourceDefinition{
		lv1.NewLonghornSettingCustomResourceDefinition(lv1.GetLonghornLables()),
		lv1.NewLonghornVolumeCustomResourceDefinition(lv1.GetLonghornLables()),
	}

	for _, crd := range crds {
		if _, err = s.crdclient.ApiextensionsV1beta1().CustomResourceDefinitions().Create(crd); err != nil && !apierrors.IsAlreadyExists(err) {
			return errors.Wrapf(err, "Creating CRD: %s", crd.Spec.Names.Kind)
		}
	}

	return util.WaitForCRDReady(listFuncs...)
}

func (s *CRDBackend) trimPrefix(key string) string {
	return strings.TrimPrefix(strings.TrimPrefix(key, s.prefix), "/")
}

var (
	volumeRegx            = regexp.MustCompile(keyVolumes + `/([^\/]+)$`)
	volumebaserRegx       = regexp.MustCompile(keyVolumes + `/([^\/]+)/` + keyVolumeBase)
	volumeControllerrRegx = regexp.MustCompile(keyVolumes + `/([^\/]+)/` + keyVolumeInstances + `/` + keyVolumeInstanceController)
	volumeReplicaRegx     = regexp.MustCompile(keyVolumes + `/([^\/]+)/` + keyVolumeInstances + `/` + keyVolumeInstanceReplicas + `/([^\/]+)`)
	nodeRegx              = regexp.MustCompile(keyNodes + `/([^\/]+)`)
	replicaKeysRegx       = regexp.MustCompile(keyVolumes + `/(\S+)/` + keyVolumeInstances + `/` + keyVolumeInstanceReplicas)
)

func (s *CRDBackend) Create(key string, obj interface{}) (uint64, error) {
	key = s.trimPrefix(key)
	logrus.Debugf("- create key = [%v]", key)

	if key == keySettings {
		// 1. settings
		if setting, err := ToSetting(obj); err != nil {
			return 0, err
		} else if _, err := s.setcli.Create(setting); err != nil {
			return 0, err
		}
	} else if strings.HasPrefix(key, keyVolumes) {
		// 2. volumes
		if fields := volumebaserRegx.FindStringSubmatch(key); len(fields) > 1 {
			// 2.2 /longhorn/volumes/vol1/base
			if volume, err := ToVolume(obj); err != nil {
				return 0, err
			} else if _, err := s.vcli.Create(volume); err != nil {
				return 0, errors.Wrapf(err, "create volume base failed : %#v", err)
			}
		} else if fields := volumeControllerrRegx.FindStringSubmatch(key); len(fields) > 1 {
			// 2.3 /longhorn/volumes/vol1/instances/controller
			volumename := fields[1]
			controllerinfo, ok := obj.(*types.ControllerInfo)
			if !ok {
				return 0, errors.Errorf("Mismatch type: %T", obj)
			}
			volume, err := s.vcli.Get(volumename, metav1.GetOptions{})
			if err != nil {
				return 0, err
			}
			if volume.Status == nil {
				volume.Status = &lv1.VolumeStatus{}
			}
			volume.Status.Controller = controllerinfo
			if _, err := s.vcli.Update(volume); err != nil {
				return 0, errors.Wrapf(err, "create controller failed : %#v", err)
			}
		} else if fields := volumeReplicaRegx.FindStringSubmatch(key); len(fields) > 2 {
			// 2.4 /longhorn/volumes/vol1/instances/replicas/{vol1}-replica-{7d3248ab-0f95-4454}
			volumename := fields[1]
			replicainfo, ok := obj.(*types.ReplicaInfo)
			if !ok {
				return 0, errors.Errorf("Mismatch type: %T", obj)
			}
			volume, err := s.vcli.Get(volumename, metav1.GetOptions{})
			if err != nil {
				return 0, err
			}
			if volume.Status == nil {
				volume.Status = &lv1.VolumeStatus{}
			}
			volume.Status.Replicas = append(volume.Status.Replicas, replicainfo)
			if _, err := s.vcli.Update(volume); err != nil {
				return 0, errors.Wrapf(err, "create replica failed : %#v", err)
			}
		}
	} else if strings.HasPrefix(key, keyNodes) {
		// do nothing here cause we just use all k8s nodes
	}

	return 0, nil
}

func (s *CRDBackend) Update(key string, obj interface{}, index uint64) (uint64, error) {
	key = s.trimPrefix(key)
	logrus.Debugf("- update key = [%v]", key)
	if key == keySettings {
		// 1. settings
		setting, err := s.lcli.LonghornV1().Settings(s.namespace).Get(keySettings, metav1.GetOptions{})
		if err != nil {
			return 0, err
		}
		info, ok := obj.(*types.SettingsInfo)
		if !ok {
			return 0, errors.Errorf("Mismatch type: %T", obj)
		}
		setting.Spec.BackupTarget = info.BackupTarget
		if _, err := s.lcli.LonghornV1().Settings(s.namespace).Update(setting); err != nil {
			return 0, err
		}
	} else if strings.HasPrefix(key, keyVolumes) {
		// 2. volumes
		if fields := volumebaserRegx.FindStringSubmatch(key); len(fields) > 1 {
			// 2.2 /longhorn/volumes/vol1/base
			volumename := fields[1]
			volumeinfo, ok := obj.(*types.VolumeInfo)
			if !ok {
				return 0, errors.Errorf("Mismatch type: %T", obj)
			}
			volume, err := s.vcli.Get(volumename, metav1.GetOptions{})
			if err != nil {
				return 0, err
			}
			volume.Spec.Volume = volumeinfo
			if _, err := s.vcli.Update(volume); err != nil {
				return 0, errors.Wrapf(err, "update volume base failed : %#v", err)
			}
		} else if fields := volumeControllerrRegx.FindStringSubmatch(key); len(fields) > 1 {
			// 2.3 /longhorn/volumes/vol1/instances/controller
			volumename := fields[1]
			controllerinfo, ok := obj.(*types.ControllerInfo)
			if !ok {
				return 0, errors.Errorf("Mismatch type: %T", obj)
			}
			volume, err := s.vcli.Get(volumename, metav1.GetOptions{})
			if err != nil {
				return 0, err
			}
			if volume.Status == nil {
				volume.Status = &lv1.VolumeStatus{}
			}
			volume.Status.Controller = controllerinfo
			if _, err := s.vcli.Update(volume); err != nil {
				return 0, errors.Wrapf(err, "update controller failed : %#v", err)
			}

		} else if fields := volumeReplicaRegx.FindStringSubmatch(key); len(fields) > 2 {
			// 2.4 /longhorn/volumes/vol1/instances/replicas/{vol1}-replica-{7d3248ab-0f95-4454}
			volumename := fields[1]
			replicainfo, ok := obj.(*types.ReplicaInfo)
			if !ok {
				return 0, errors.Errorf("Mismatch type: %T", obj)
			}
			volume, err := s.vcli.Get(volumename, metav1.GetOptions{})
			if err != nil {
				return 0, err
			}
			if volume.Status == nil {
				volume.Status = &lv1.VolumeStatus{}
			}
			replicas := []*types.ReplicaInfo{replicainfo}
			for _, replica := range volume.Status.Replicas {
				if replicainfo.Name == replica.Name {
					continue
				}
				replicas = append(replicas, replica)
			}
			volume.Status.Replicas = replicas
			if _, err := s.vcli.Update(volume); err != nil {
				return 0, errors.Wrapf(err, "update replica failed : %#v", err)
			}
		}
	} else if strings.HasPrefix(key, keyNodes) {
		// do nothing here cause we just use all k8s nodes
	}
	return 0, nil
}

func (s *CRDBackend) IsNotFoundError(err error) bool {
	return apierrors.IsNotFound(err)
}

func (s *CRDBackend) Get(key string, obj interface{}) (uint64, error) {
	key = s.trimPrefix(key)
	logrus.Debugf("- get key = [%v]", key)
	if key == keySettings {
		// 1. settings
		setting, err := s.lcli.LonghornV1().Settings(s.namespace).Get(keySettings, metav1.GetOptions{})
		if err != nil {
			return 0, err
		}
		info, ok := obj.(*types.SettingsInfo)
		if !ok {
			return 0, errors.Errorf("Mismatch type: %T", obj)
		}
		info.BackupTarget = setting.Spec.BackupTarget
	} else if strings.HasPrefix(key, keyVolumes) {
		// 2. volumes
		if fields := volumebaserRegx.FindStringSubmatch(key); len(fields) > 1 {
			// 2.2 /longhorn/volumes/vol1/base
			volumename := fields[1]
			volumeinfo, ok := obj.(*types.VolumeInfo)
			if !ok {
				return 0, errors.Errorf("Mismatch type: %T", obj)
			}
			volume, err := s.vcli.Get(volumename, metav1.GetOptions{})
			if err != nil {
				logrus.Debugf("-- err = [%#v]", err)
				if apierrors.IsNotFound(err) {
					logrus.Debugf("-- err is not found")
				}
				return 0, err
			}
			*volumeinfo = *volume.Spec.Volume
		} else if fields := volumeControllerrRegx.FindStringSubmatch(key); len(fields) > 1 {
			// 2.3 /longhorn/volumes/vol1/instances/controller
			volumename := fields[1]
			controllerinfo, ok := obj.(*types.ControllerInfo)
			if !ok {
				return 0, errors.Errorf("Mismatch type: %T", obj)
			}
			volume, err := s.vcli.Get(volumename, metav1.GetOptions{})
			if err != nil {
				return 0, err
			}
			if volume.Status == nil || volume.Status.Controller == nil {
				return 0, apierrors.NewNotFound(apiv1.Resource("controller"), key)
			}
			*controllerinfo = *volume.Status.Controller
		} else if fields := volumeReplicaRegx.FindStringSubmatch(key); len(fields) > 2 {
			// 2.4 /longhorn/volumes/vol1/instances/replicas/{vol1}-replica-{7d3248ab-0f95-4454}
			volumename := fields[1]
			replicaname := fields[2]
			replicainfo, ok := obj.(*types.ReplicaInfo)
			if !ok {
				return 0, errors.Errorf("Mismatch type: %T", obj)
			}
			volume, err := s.vcli.Get(volumename, metav1.GetOptions{})
			if err != nil {
				return 0, err
			}
			if volume.Status == nil {
				return 0, apierrors.NewNotFound(apiv1.Resource("replica"), key)
			}
			for _, replica := range volume.Status.Replicas {
				if replica.Name == replicaname {
					*replicainfo = *replica
					break
				}
			}
		}
	} else if fields := nodeRegx.FindStringSubmatch(key); len(fields) > 1 {
		nodeID := fields[1]
		nodeinfo, ok := obj.(*types.NodeInfo)
		if !ok {
			return 0, errors.Errorf("Mismatch type: %T", obj)
		}
		node, err := s.kcli.Core().Nodes().Get(nodeID, metav1.GetOptions{})
		if err != nil {
			return 0, err
		}
		var ip string
		for _, address := range node.Status.Addresses {
			if address.Type == apiv1.NodeExternalIP {
				ip = address.Address
			}
			if address.Type == apiv1.NodeInternalIP && ip == "" {
				ip = address.Address
			}
		}
		nodetmp := types.NodeInfo{
			ID:               node.GetName(),
			Name:             node.GetName(),
			IP:               ip,
			ManagerPort:      9507,
			OrchestratorPort: 9508,
			State:            types.NodeStateUp,
		}
		*nodeinfo = nodetmp
	}

	return 0, nil
}

func (s *CRDBackend) Keys(prefix string) ([]string, error) {
	prefix = s.trimPrefix(prefix)
	logrus.Debugf("- list key = [%v]", prefix)
	ret := []string{}
	if prefix == keyVolumes {
		vs, err := s.vcli.List(metav1.ListOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return nil, nil
			}
			return nil, err
		}
		for _, v := range vs.(*lv1.VolumeList).Items {
			ret = append(ret, filepath.Join(s.prefix, keyVolumes, v.GetName()))
		}
	} else if fields := replicaKeysRegx.FindStringSubmatch(prefix); len(fields) > 1 {
		volumename := fields[1]
		volume, err := s.vcli.Get(volumename, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return nil, nil
			}
			return nil, err
		}
		if volume.Status == nil {
			return nil, nil
		}
		for _, replica := range volume.Status.Replicas {
			ret = append(ret, filepath.Join(s.prefix, keyVolumes, volumename, keyVolumeInstances, keyVolumeInstanceReplicas, replica.Name))
		}
	} else if prefix == keyNodes {
		nodes, err := s.kcli.Core().Nodes().List(metav1.ListOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return nil, nil
			}
			return nil, err
		}
		for _, node := range nodes.Items {
			for _, cond := range node.Status.Conditions {
				if cond.Type == apiv1.NodeReady && cond.Status == apiv1.ConditionTrue {
					ret = append(ret, filepath.Join(s.prefix, keyNodes, node.GetName()))
					break
				}
			}
		}
	}
	return ret, nil
}

func (s *CRDBackend) Delete(key string) error {
	key = s.trimPrefix(key)
	logrus.Debugf("- delete key = [%v]", key)
	if fields := volumeRegx.FindStringSubmatch(key); len(fields) > 1 {
		volumename := fields[1]
		if err := s.vcli.Delete(volumename, &metav1.DeleteOptions{}); err != nil {
			if !apierrors.IsNotFound(err) {
				return err
			}
		}
	} else if fields := volumeControllerrRegx.FindStringSubmatch(key); len(fields) > 1 {
		volumename := fields[1]
		volume, err := s.vcli.Get(volumename, metav1.GetOptions{})
		if err != nil {
			if !apierrors.IsNotFound(err) {
				return err
			}
		}
		volume.Status.Controller = nil
		if _, err := s.vcli.Update(volume); err != nil {
			if !apierrors.IsNotFound(err) {
				return err
			}
		}
	} else if fields := volumeReplicaRegx.FindStringSubmatch(key); len(fields) > 2 {
		volumename := fields[1]
		replicaname := fields[2]
		volume, err := s.vcli.Get(volumename, metav1.GetOptions{})
		if err != nil {
			if !apierrors.IsNotFound(err) {
				return err
			}
		}
		replicas := []*types.ReplicaInfo{}
		for _, replica := range volume.Status.Replicas {
			if replica.Name == replicaname {
				continue
			}
			replicas = append(replicas, replica)
		}
		volume.Status.Replicas = replicas
		if _, err := s.vcli.Update(volume); err != nil {
			if !apierrors.IsNotFound(err) {
				return err
			}
		}
	}

	return nil
}
