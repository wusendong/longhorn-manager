package api

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"

	"github.com/rancher/longhorn-manager/manager"
	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/util"
)

func (s *Server) VolumeList(rw http.ResponseWriter, req *http.Request) (err error) {
	defer func() {
		err = errors.Wrap(err, "unable to list")
	}()

	apiContext := api.GetApiContext(req)

	resp := &client.GenericCollection{}

	volumes, err := s.m.VolumeCRList()
	if err != nil {
		return err
	}

	for _, v := range volumes {
		volume := types.VolumeInfo{VolumeSpecInfo: *v.Spec.Volume, VolumeRunningInfo: *v.Status.Volume}
		replicas := map[string]*types.ReplicaInfo{}
		for _, replica := range v.Status.Replicas {
			replicas[replica.Name] = replica
		}
		resp.Data = append(resp.Data, toVolumeResource(&volume, v.Status.Controller, replicas, apiContext))
	}
	resp.ResourceType = "volume"
	resp.CreateTypes = map[string]string{
		"volume": apiContext.UrlBuilder.Collection("volume"),
	}
	apiContext.Write(resp)

	return nil
}

func (s *Server) VolumeGet(rw http.ResponseWriter, req *http.Request) error {
	id := mux.Vars(req)["name"]
	return s.responseWithVolume(rw, req, id)
}

func (s *Server) responseWithVolume(rw http.ResponseWriter, req *http.Request, id string) error {
	apiContext := api.GetApiContext(req)

	v, err := s.m.VolumeCRInfo(id)
	if err != nil {
		return errors.Wrap(err, "unable to get volume")
	}

	volume := types.VolumeInfo{VolumeSpecInfo: *v.Spec.Volume, VolumeRunningInfo: *v.Status.Volume}
	replicas := map[string]*types.ReplicaInfo{}
	for _, replica := range v.Status.Replicas {
		replicas[replica.Name] = replica
	}

	apiContext.Write(toVolumeResource(&volume, v.Status.Controller, replicas, apiContext))
	return nil
}

func (s *Server) VolumeCreate(rw http.ResponseWriter, req *http.Request) error {
	var v Volume
	apiContext := api.GetApiContext(req)

	if err := apiContext.Read(&v); err != nil {
		return err
	}

	request, err := generateVolumeCreateRequest(&v)
	if err != nil {
		return errors.Wrap(err, "unable to filter create volume input")
	}

	if err := s.m.VolumeCreate(request); err != nil {
		return errors.Wrap(err, "unable to create volume")
	}
	return s.responseWithVolume(rw, req, v.Name)
}

func (s *Server) VolumeDelete(rw http.ResponseWriter, req *http.Request) error {
	id := mux.Vars(req)["name"]

	if err := s.m.VolumeDelete(&manager.VolumeDeleteRequest{
		Name: id,
	}); err != nil {
		return errors.Wrap(err, "unable to delete volume")
	}

	return nil
}

func generateVolumeCreateRequest(v *Volume) (*manager.VolumeCreateRequest, error) {
	size, err := util.ConvertSize(v.Size)
	if err != nil {
		return nil, errors.Wrapf(err, "error converting size '%s'", v.Size)
	}
	return &manager.VolumeCreateRequest{
		Name:                v.Name,
		Size:                strconv.FormatInt(util.RoundUpSize(size), 10),
		BaseImage:           v.BaseImage,
		FromBackup:          v.FromBackup,
		NumberOfReplicas:    v.NumberOfReplicas,
		StaleReplicaTimeout: v.StaleReplicaTimeout,
	}, nil
}

func (s *Server) VolumeAttach(rw http.ResponseWriter, req *http.Request) error {
	var input AttachInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	id := mux.Vars(req)["name"]

	if err := s.m.VolumeAttach(&manager.VolumeAttachRequest{
		Name:   id,
		NodeID: input.HostID,
	}); err != nil {
		return errors.Wrap(err, "unable to attach volume")
	}

	return s.responseWithVolume(rw, req, id)
}

func (s *Server) VolumeDetach(rw http.ResponseWriter, req *http.Request) error {
	id := mux.Vars(req)["name"]

	if err := s.m.VolumeDetach(&manager.VolumeDetachRequest{
		Name: id,
	}); err != nil {
		return errors.Wrap(err, "unable to detach volume")
	}

	return s.responseWithVolume(rw, req, id)
}

func (s *Server) VolumeSalvage(rw http.ResponseWriter, req *http.Request) error {
	var input SalvageInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return errors.Wrapf(err, "error read replicaRemoveInput")
	}

	id := mux.Vars(req)["name"]

	if err := s.m.VolumeSalvage(&manager.VolumeSalvageRequest{
		Name:                id,
		SalvageReplicaNames: input.Names,
	}); err != nil {
		return errors.Wrap(err, "unable to remove replica")
	}

	return s.responseWithVolume(rw, req, id)
}

func (s *Server) VolumeRecurringUpdate(rw http.ResponseWriter, req *http.Request) error {
	var input RecurringInput
	id := mux.Vars(req)["name"]

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return errors.Wrapf(err, "error reading recurringInput")
	}

	for _, job := range input.Jobs {
		if job.Cron == "" || job.Type == "" || job.Name == "" || job.Retain == 0 {
			return fmt.Errorf("invalid job %+v", job)
		}
	}

	if err := s.m.VolumeRecurringUpdate(&manager.VolumeRecurringUpdateRequest{
		Name:          id,
		RecurringJobs: input.Jobs,
	}); err != nil {
		return errors.Wrapf(err, "unable to update recurring jobs for volume %v", id)
	}

	return s.responseWithVolume(rw, req, id)
}

func (s *Server) ReplicaRemove(rw http.ResponseWriter, req *http.Request) error {
	var input ReplicaRemoveInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return errors.Wrapf(err, "error read replicaRemoveInput")
	}

	id := mux.Vars(req)["name"]

	if err := s.m.ReplicaRemove(id, input.Name); err != nil {
		return errors.Wrap(err, "unable to remove replica")
	}

	return s.responseWithVolume(rw, req, id)
}

func (s *Server) JobList(rw http.ResponseWriter, req *http.Request) error {
	id := mux.Vars(req)["name"]

	apiContext := api.GetApiContext(req)

	jobs, err := s.m.JobList(id)
	if err != nil {
		return errors.Wrap(err, "unable to list jobs")
	}

	apiContext.Write(toJobCollection(jobs))
	return nil
}
