package k8sapi

import "k8s.io/apimachinery/pkg/runtime"

type WatcherOpt[T runtime.Object] func(watcher *Watcher[T])

// namespace to be watched. An empty string watches all namespaces.
func WithNamespace[T runtime.Object](namespace string) WatcherOpt[T] {
	return func(watcher *Watcher[T]) {
		watcher.namespace = namespace
	}
}

// equals func checks if a new obj is equal to a cached obj. If true is returned,
// an update is not triggered. If this func is nil, an update is always triggered.
func WithEquals[T runtime.Object](equals func(T, T) bool) WatcherOpt[T] {
	return func(watcher *Watcher[T]) {
		watcher.equals = equals
	}
}

func WithLabelSelector[T runtime.Object](labelSelector string) WatcherOpt[T] {
	return func(watcher *Watcher[T]) {
		watcher.labelSelector = labelSelector
	}
}

func WithFieldSelector[T runtime.Object](fieldSelector string) WatcherOpt[T] {
	return func(watcher *Watcher[T]) {
		watcher.fieldSelector = fieldSelector
	}
}
