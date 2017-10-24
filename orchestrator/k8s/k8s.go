package k8s

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	apiv1 "k8s.io/client-go/pkg/api/v1"
	"k8s.io/client-go/rest"

	"github.com/rancher/longhorn-manager/orchestrator"
	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/util"
)

const (
	cfgDirectory = "/var/lib/rancher/longhorn/"
	hostUUIDFile = cfgDirectory + ".physical_host_uuid"
)

var (
	ContainerStopTimeout = 1 * time.Minute
	WaitDeviceTimeout    = 30 //seconds
	WaitAPITimeout       = 30 //seconds

	GraceStopTimeout = 100 * time.Millisecond
)

type K8s struct {
	EngineImage string
	Namespace   string
	IP          string

	currentNode *types.NodeInfo

	cli *kubernetes.Clientset
}

type Config struct {
	EngineImage string
	Namespace   string
}

func NewK8sOrchestrator(cfg *Config) (*K8s, error) {
	var err error

	if cfg.EngineImage == "" {
		return nil, fmt.Errorf("missing required parameter EngineImage")
	}
	if cfg.Namespace == "" {
		if namespace := os.Getenv("MY_POD_NAMESPACE"); namespace != "" {
			cfg.Namespace = namespace
		} else {
			cfg.Namespace = apiv1.NamespaceDefault
		}
	}
	k8s := &K8s{
		EngineImage: cfg.EngineImage,
		Namespace:   cfg.Namespace,
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, errors.Wrap(err, "cannot connect to k8s, may be not in k8s cluster")
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, errors.Wrap(err, "cannot connect to k8s")
	}
	k8s.cli = clientset

	if _, err := k8s.cli.Core().Pods(cfg.Namespace).List(metav1.ListOptions{}); err != nil {
		return nil, errors.Wrap(err, "cannot pass test to get pod list")
	}

	if err = k8s.updateNetwork(); err != nil {
		return nil, errors.Wrapf(err, "fail to detect dedicated container network: %v", cfg.Namespace)
	}

	logrus.Infof("Detected namespace is %s, IP is %s", k8s.Namespace, k8s.IP)

	if err := k8s.updateCurrentNode(); err != nil {
		return nil, err
	}

	logrus.Info("K8s orchestrator is ready")
	return k8s, nil
}

func (d *K8s) updateNetwork() error {

	if ip := os.Getenv("MY_POD_IP"); ip != "" {
		d.IP = ip
		return nil
	}
	containerID := os.Getenv("HOSTNAME")

	pod, err := d.cli.Core().Pods(d.Namespace).Get(containerID, metav1.GetOptions{})
	if err != nil {
		return errors.Errorf("could not get manager pod")
	}
	if pod.Status.PodIP == "" {
		return errors.Errorf("could not obtain manager ip")
	}
	d.IP = pod.Status.PodIP
	return nil
}

func (d *K8s) updateCurrentNode() error {
	var err error

	node := &types.NodeInfo{
		IP:               d.IP,
		OrchestratorPort: types.DefaultOrchestratorPort,
	}

	if nodename := os.Getenv("MY_NODE_NAME"); nodename != "" {
		node.Name = nodename
	} else {
		node.Name, err = os.Hostname()
		if err != nil {
			return err
		}
	}

	uuid, err := ioutil.ReadFile(hostUUIDFile)
	if err == nil {
		node.ID = string(uuid)
		d.currentNode = node
		return nil
	}

	// file doesn't exists, generate new UUID for the host
	node.ID = util.UUID()
	if err := os.MkdirAll(cfgDirectory, os.ModeDir|0600); err != nil {
		return fmt.Errorf("Fail to create configuration directory: %v", err)
	}
	if err := ioutil.WriteFile(hostUUIDFile, []byte(node.ID), 0600); err != nil {
		return fmt.Errorf("Fail to write host uuid file: %v", err)
	}
	d.currentNode = node
	return nil
}

func (d *K8s) GetCurrentNode() *types.NodeInfo {
	return d.currentNode
}

func (d *K8s) CreateController(req *orchestrator.Request) (instance *orchestrator.Instance, err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to create controller for %v", req.VolumeName)
	}()
	if err := orchestrator.ValidateRequestCreateController(req); err != nil {
		return nil, err
	}
	if req.NodeID != d.currentNode.ID {
		return nil, fmt.Errorf("incorrect node, requested %v, current %v", req.NodeID,
			d.currentNode.ID)
	}

	cmd := []string{
		"launch", "controller",
		"--listen", "0.0.0.0:9501",
		"--frontend", "tgt",
	}
	for _, url := range req.ReplicaURLs {
		waitURL := strings.Replace(url, "tcp://", "http://", 1) + "/v1"
		if err := util.WaitForAPI(waitURL, WaitAPITimeout); err != nil {
			return nil, err
		}
		cmd = append(cmd, "--replica", url)
	}
	cmd = append(cmd, req.VolumeName)

	podClient := d.cli.Core().Pods(d.Namespace)
	pod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: req.InstanceName,
		},
		Spec: apiv1.PodSpec{
			Containers: []apiv1.Container{
				{
					Name:  req.InstanceName,
					Image: d.EngineImage,
					Args:  cmd,
					VolumeMounts: []apiv1.VolumeMount{
						apiv1.VolumeMount{
							Name:      "longhorn-controller-claim0",
							MountPath: "/host/dev",
						},
						apiv1.VolumeMount{
							Name:      "longhorn-controller-claim1",
							MountPath: "/host/proc",
						},
					},
					SecurityContext:          &apiv1.SecurityContext{Privileged: boolPtr(true)},
					TerminationMessagePolicy: apiv1.TerminationMessageFallbackToLogsOnError,
				},
			},
			Volumes: []apiv1.Volume{
				apiv1.Volume{
					Name: "longhorn-controller-claim0",
					VolumeSource: apiv1.VolumeSource{
						HostPath: &apiv1.HostPathVolumeSource{Path: "/dev"},
					},
				},
				apiv1.Volume{
					Name: "longhorn-controller-claim1",
					VolumeSource: apiv1.VolumeSource{
						HostPath: &apiv1.HostPathVolumeSource{Path: "/proc"},
					},
				},
			},
			TerminationGracePeriodSeconds: int64Ptr(2),
			RestartPolicy:                 apiv1.RestartPolicyNever,
		},
	}

	// Create Pod
	fmt.Println("Creating pod...")
	result, err := podClient.Create(pod)
	if err != nil {
		return nil, err
	}
	fmt.Printf("Created pod %q.\n", result.GetObjectMeta().GetName())

	defer func() {
		if err != nil {
			logrus.Errorf("fail to start controller %v of %v, cleaning up: %v",
				req.InstanceName, req.VolumeName, err)
			d.StopInstance(req)
			d.DeleteInstance(req)
		}
	}()

	req.InstanceID = result.GetName()
	instance, err = d.StartInstance(req)
	if err != nil {
		return nil, err
	}

	url := "http://" + instance.IP + ":9501/v1"
	if err := util.WaitForAPI(url, WaitAPITimeout); err != nil {
		return nil, err
	}

	if err := util.WaitForDevice(d.getDeviceName(req.VolumeName), WaitDeviceTimeout); err != nil {
		return nil, err
	}

	return instance, nil
}

func (d *K8s) getDeviceName(volumeName string) string {
	return filepath.Join("/dev/longhorn/", volumeName)
}

func (d *K8s) CreateReplica(req *orchestrator.Request) (instance *orchestrator.Instance, err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to create replica %v for %v",
			req.InstanceName, req.VolumeName)
	}()

	if err := orchestrator.ValidateRequestCreateReplica(req); err != nil {
		return nil, err
	}
	if req.NodeID != d.currentNode.ID {
		return nil, fmt.Errorf("incorrect node, requested %v, current %v", req.NodeID,
			d.currentNode.ID)
	}

	cmd := []string{
		"launch", "replica",
		"--listen", "0.0.0.0:9502",
		"--size", strconv.FormatInt(req.VolumeSize, 10),
	}
	if req.RestoreFrom != "" && req.RestoreName != "" {
		cmd = append(cmd, "--restore-from", req.RestoreFrom, "--restore-name", req.RestoreName)
	}
	cmd = append(cmd, "/volume")
	podClient := d.cli.Core().Pods(d.Namespace)
	pod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: req.InstanceName,
		},
		Spec: apiv1.PodSpec{
			Containers: []apiv1.Container{
				{
					Name:  req.InstanceName,
					Image: d.EngineImage,
					Args:  cmd,
					VolumeMounts: []apiv1.VolumeMount{
						apiv1.VolumeMount{
							Name:      "longhorn-replica-claim0",
							MountPath: "/volume",
						},
					},
					SecurityContext:          &apiv1.SecurityContext{Privileged: boolPtr(true)},
					TerminationMessagePolicy: apiv1.TerminationMessageFallbackToLogsOnError,
				},
			},
			Volumes: []apiv1.Volume{
				apiv1.Volume{
					Name: "longhorn-replica-claim0",
					VolumeSource: apiv1.VolumeSource{
						EmptyDir: &apiv1.EmptyDirVolumeSource{},
					},
				},
			},
			TerminationGracePeriodSeconds: int64Ptr(2),
			RestartPolicy:                 apiv1.RestartPolicyNever,
		},
	}
	// Create Pod
	fmt.Println("Creating pod...")
	result, err := podClient.Create(pod)
	if err != nil {
		return nil, err
	}
	fmt.Printf("Created pod %q.\n", result.GetObjectMeta().GetName())

	req.InstanceID = result.GetName()

	defer func() {
		if err != nil {
			d.StopInstance(req)
			d.DeleteInstance(req)
		}
	}()

	instance, err = d.InspectInstance(req)
	if err != nil {
		logrus.Errorf("fail to inspect when create replica %v of %v, cleaning up: %v", req.InstanceName, req.VolumeName, err)
		return nil, err
	}

	// make sure replica is initialized, especially for restoring backup
	instance, err = d.StartInstance(req)
	if err != nil {
		logrus.Errorf("fail to start when create replica %v of %v, cleaning up: %v", req.InstanceName, req.VolumeName, err)
		return nil, err
	}

	timeout := WaitAPITimeout
	// More time for backup restore, may need to customerize it
	if req.RestoreFrom != "" && req.RestoreName != "" {
		timeout = timeout * 10
	}
	url := "http://" + instance.IP + ":9502/v1"
	if err := util.WaitForAPI(url, timeout); err != nil {
		return nil, err
	}

	instance, err = d.StopInstance(req)
	if err != nil {
		logrus.Errorf("fail to stop when create replica %v of %v, cleaning up: %v", req.InstanceName, req.VolumeName, err)
		return nil, err
	}

	return instance, nil
}

func (d *K8s) InspectInstance(req *orchestrator.Request) (instance *orchestrator.Instance, err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to inspect instance %v(%v)", req.InstanceName, req.InstanceID)
	}()

	if err := orchestrator.ValidateRequestInstanceOps(req); err != nil {
		return nil, err
	}
	if req.NodeID != d.currentNode.ID {
		return nil, fmt.Errorf("incorrect node, requested %v, current %v", req.NodeID,
			d.currentNode.ID)
	}

	pod, err := d.cli.Core().Pods(d.Namespace).Get(req.InstanceID, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	instance = &orchestrator.Instance{
		// It's weird that Docker put a forward slash to the container name
		// So it become "/replica-1"
		ID:      pod.GetName(),
		Name:    pod.GetName(),
		Running: pod.Status.Phase == "Running",
		NodeID:  d.GetCurrentNode().ID,
	}
	instance.IP = pod.Status.PodIP
	if instance.Running && instance.IP == "" {
		msg := fmt.Sprintf("BUG: Cannot find IP address of %v", instance.ID)
		logrus.Errorf(msg)
		return nil, errors.Errorf(msg)
	}
	return instance, nil
}

func (d *K8s) StartInstance(req *orchestrator.Request) (instance *orchestrator.Instance, err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to start instance %v(%v)", req.InstanceName, req.InstanceID)
	}()

	if err := orchestrator.ValidateRequestInstanceOps(req); err != nil {
		return nil, err
	}
	if req.NodeID != d.currentNode.ID {
		return nil, fmt.Errorf("incorrect node, requested %v, current %v", req.NodeID,
			d.currentNode.ID)
	}

	fmt.Printf("waitting pod name: %v\n", req.InstanceID)
	pod, err := d.cli.Core().Pods(d.Namespace).Get(req.InstanceID, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if err = d.WaitForPodsReady(pod); err != nil {
		return nil, err
	}

	return d.InspectInstance(req)
}

func (d *K8s) StopInstance(req *orchestrator.Request) (instance *orchestrator.Instance, err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to stop instance %v(%v)", req.InstanceName, req.InstanceID)
	}()

	if err := orchestrator.ValidateRequestInstanceOps(req); err != nil {
		return nil, err
	}
	if req.NodeID != d.currentNode.ID {
		return nil, fmt.Errorf("incorrect node, requested %v, current %v", req.NodeID,
			d.currentNode.ID)
	}

	return d.InspectInstance(req)
}

func (d *K8s) DeleteInstance(req *orchestrator.Request) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to delete instance %v(%v)", req.InstanceName, req.InstanceID)
	}()

	if err := orchestrator.ValidateRequestInstanceOps(req); err != nil {
		return err
	}
	if req.NodeID != d.currentNode.ID {
		return fmt.Errorf("incorrect node, requested %v, current %v", req.NodeID,
			d.currentNode.ID)
	}

	return d.cli.Core().Pods(d.Namespace).Delete(req.InstanceID, &metav1.DeleteOptions{})
}

func int32Ptr(i int32) *int32 { return &i }
func int64Ptr(i int64) *int64 { return &i }
func boolPtr(i bool) *bool    { return &i }

// WaitForPodsReady waits for a selection of Pods to be running and each
// container to pass its readiness check.
func (d *K8s) WaitForPodsReady(p *apiv1.Pod) error {
	return wait.Poll(time.Second, 30*time.Second, func() (bool, error) {
		pod, err := d.cli.Core().Pods(d.Namespace).Get(p.GetName(), metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		return PodRunningAndReady(pod)
	})
}

// PodRunningAndReady returns whether a pod is running and each container has
// passed it's ready state.
func PodRunningAndReady(pod *apiv1.Pod) (bool, error) {
	switch pod.Status.Phase {
	case apiv1.PodFailed, apiv1.PodSucceeded:
		return false, fmt.Errorf("pod completed")
	case apiv1.PodRunning:
		for _, cond := range pod.Status.Conditions {
			if cond.Type != apiv1.PodReady {
				continue
			}
			return cond.Status == apiv1.ConditionTrue, nil
		}
		return false, fmt.Errorf("pod ready condition not found")
	}
	return false, nil
}
