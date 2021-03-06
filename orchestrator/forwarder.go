package orchestrator

import (
	"fmt"
	"net"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/rancher/longhorn-manager/orchestrator/pb"
	"github.com/rancher/longhorn-manager/types"
)

type Forwarder struct {
	orch    Orchestrator
	locator NodeLocator
	done    chan struct{}
}

func NewForwarder(orch Orchestrator) *Forwarder {
	return &Forwarder{
		orch: orch,
		done: make(chan struct{}),
	}
}

func (f *Forwarder) SetLocator(locator NodeLocator) {
	f.locator = locator
}

func (f *Forwarder) StartServer(address string) error {
	if f.locator == nil {
		return fmt.Errorf("BUG: locator wasn't set for forwarder")
	}

	l, err := net.Listen("tcp", address)
	if err != nil {
		return errors.Wrap(err, "fail to start orchestrator forwarder GRPC server")
	}
	s := grpc.NewServer()
	pb.RegisterForwarderServer(s, f)
	reflection.Register(s)
	go func() {
		if err := s.Serve(l); err != nil {
			logrus.Errorf("fail to serve orchestrator forwarder GRPC server: %v", err)
		}
	}()
	go func() {
		<-f.done
		s.GracefulStop()
	}()
	return nil
}

func (f *Forwarder) StopServer() {
	close(f.done)
}

func (f *Forwarder) InstanceOperationRPC(ctx context.Context, req *pb.InstanceOperationRequest) (*pb.InstanceOperationResponse, error) {
	var (
		instance *Instance
		err      error
		resp     *pb.InstanceOperationResponse
	)

	request := &Request{
		NodeID:       req.NodeID,
		InstanceID:   req.InstanceID,
		InstanceName: req.InstanceName,
		VolumeName:   req.VolumeName,
		VolumeSize:   req.VolumeSize,
		ReplicaURLs:  req.ReplicaURLs,
		RestoreFrom:  req.RestoreFrom,
		RestoreName:  req.RestoreName,
	}
	switch InstanceOperationType(req.Type) {
	case InstanceOperationTypeCreateController:
		instance, err = f.orch.CreateController(request)
	case InstanceOperationTypeCreateReplica:
		instance, err = f.orch.CreateReplica(request)
	case InstanceOperationTypeStartInstance:
		instance, err = f.orch.StartInstance(request)
	case InstanceOperationTypeStopInstance:
		instance, err = f.orch.StopInstance(request)
	case InstanceOperationTypeDeleteInstance:
		err = f.orch.DeleteInstance(request)
	case InstanceOperationTypeInspectInstance:
		instance, err = f.orch.InspectInstance(request)
	default:
		err = fmt.Errorf("invalid instance operation type: %v", req.Type)
	}
	if err != nil {
		return nil, err
	}
	// avoid nil response
	resp = &pb.InstanceOperationResponse{
		InstanceID: req.InstanceID,
	}
	if instance != nil {
		resp = &pb.InstanceOperationResponse{
			InstanceID:   instance.ID,
			InstanceName: instance.Name,
			Running:      instance.Running,
			Ip:           instance.IP,
			NodeID:       instance.NodeID,
		}
	}
	return resp, nil
}

func (f *Forwarder) CreateController(request *Request) (*Instance, error) {
	if request.NodeID == f.orch.GetCurrentNode().ID {
		return f.orch.CreateController(request)
	}
	if f.locator == nil {
		return nil, fmt.Errorf("BUG: locator wasn't set for forwarder")
	}
	address, err := f.locator.Node2OrchestratorAddress(request.NodeID)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create controller")
	}
	return f.InstanceOperation(address, InstanceOperationTypeCreateController, request)
}

func (f *Forwarder) CreateReplica(request *Request) (*Instance, error) {
	if request.NodeID == f.orch.GetCurrentNode().ID {
		return f.orch.CreateReplica(request)
	}
	if f.locator == nil {
		return nil, fmt.Errorf("BUG: locator wasn't set for forwarder")
	}
	address, err := f.locator.Node2OrchestratorAddress(request.NodeID)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create replica")
	}
	return f.InstanceOperation(address, InstanceOperationTypeCreateReplica, request)
}

func (f *Forwarder) StartInstance(request *Request) (*Instance, error) {
	if request.NodeID == f.orch.GetCurrentNode().ID {
		return f.orch.StartInstance(request)
	}
	if f.locator == nil {
		return nil, fmt.Errorf("BUG: locator wasn't set for forwarder")
	}
	address, err := f.locator.Node2OrchestratorAddress(request.NodeID)
	if err != nil {
		return nil, errors.Wrap(err, "unable to start instance")
	}
	return f.InstanceOperation(address, InstanceOperationTypeStartInstance, request)
}

func (f *Forwarder) StopInstance(request *Request) (*Instance, error) {
	if request.NodeID == f.orch.GetCurrentNode().ID {
		return f.orch.StopInstance(request)
	}
	if f.locator == nil {
		return nil, fmt.Errorf("BUG: locator wasn't set for forwarder")
	}
	address, err := f.locator.Node2OrchestratorAddress(request.NodeID)
	if err != nil {
		return nil, errors.Wrap(err, "unable to stop instance")
	}
	return f.InstanceOperation(address, InstanceOperationTypeStopInstance, request)
}

func (f *Forwarder) DeleteInstance(request *Request) error {
	if request.NodeID == f.orch.GetCurrentNode().ID {
		return f.orch.DeleteInstance(request)
	}
	if f.locator == nil {
		return fmt.Errorf("BUG: locator wasn't set for forwarder")
	}
	address, err := f.locator.Node2OrchestratorAddress(request.NodeID)
	if err != nil {
		return errors.Wrap(err, "unable to delete instance")
	}
	if _, err := f.InstanceOperation(address, InstanceOperationTypeDeleteInstance, request); err != nil {
		return err
	}
	return nil
}

func (f *Forwarder) InspectInstance(request *Request) (*Instance, error) {
	if request.NodeID == f.orch.GetCurrentNode().ID {
		return f.orch.InspectInstance(request)
	}
	if f.locator == nil {
		return nil, fmt.Errorf("BUG: locator wasn't set for forwarder")
	}
	address, err := f.locator.Node2OrchestratorAddress(request.NodeID)
	if err != nil {
		return nil, errors.Wrap(err, "unable to inspect instance")
	}
	return f.InstanceOperation(address, InstanceOperationTypeInspectInstance, request)
}

func (f *Forwarder) GetCurrentNode() *types.NodeInfo {
	return f.orch.GetCurrentNode()
}

func (f *Forwarder) InstanceOperation(address string, opType InstanceOperationType, request *Request) (*Instance, error) {
	var instance *Instance

	//FIXME insecure
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return nil, errors.Wrapf(err, "fail to connect to %v", address)
	}
	defer conn.Close()
	c := pb.NewForwarderClient(conn)

	resp, err := c.InstanceOperationRPC(context.Background(), &pb.InstanceOperationRequest{
		Type:         string(opType),
		NodeID:       request.NodeID,
		InstanceID:   request.InstanceID,
		InstanceName: request.InstanceName,
		VolumeName:   request.VolumeName,
		VolumeSize:   request.VolumeSize,
		ReplicaURLs:  request.ReplicaURLs,
		RestoreFrom:  request.RestoreFrom,
		RestoreName:  request.RestoreName,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "fail to execute instance operation on %v", address)
	}
	if resp != nil {
		instance = &Instance{
			ID:      resp.InstanceID,
			Name:    resp.InstanceName,
			NodeID:  resp.NodeID,
			Running: resp.Running,
			IP:      resp.Ip,
		}
	}
	return instance, nil
}
