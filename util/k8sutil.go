package util

import (
	"fmt"
	"net/http"
	"time"

	"github.com/pkg/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
)

// WaitForCRDReady waits for a third party resource to be available for use.
func WaitForCRDReady(listFuncs ...func(opts metav1.ListOptions) (runtime.Object, error)) error {
	for _, listFunc := range listFuncs {
		err := wait.Poll(3*time.Second, 10*time.Minute, func() (bool, error) {
			return CrdExists(listFunc)
		})
		return errors.Wrap(err, fmt.Sprintf("timed out waiting for Custom Resoruce"))
	}
	return nil
}

func CrdExists(listFuncs ...func(opts metav1.ListOptions) (runtime.Object, error)) (bool, error) {
	for _, listFunc := range listFuncs {
		_, err := listFunc(metav1.ListOptions{})
		if err != nil {
			if se, ok := err.(*apierrors.StatusError); ok {
				if se.Status().Code == http.StatusNotFound {
					return false, nil
				}
			}
			return false, err
		}
	}
	return true, nil
}
