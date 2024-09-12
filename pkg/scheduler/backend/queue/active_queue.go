/*
Copyright 2024 The Kubernetes Authors.

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

package queue

import (
	"container/list"
	"fmt"
	"sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/backend/heap"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
)

// activeQueuer is a wrapper for activeQ related operations. - activeQueuer 是 activeQ 相关操作的包装器。
// Its methods, except "unlocked" ones, take the lock inside. - 它的方法，除了 "unlocked" 方法外，都在内部获取锁。
// Note: be careful when using unlocked() methods. - 注意：使用 unlocked() 方法时要小心。
// getLock() methods should be used only for unlocked() methods
// and it is forbidden to call any other activeQueuer's method under this lock. - 获取锁的方法只能用于 unlocked() 方法，并且禁止在获取锁时调用任何其他 activeQueuer 的方法。
type activeQueuer interface {
	underLock(func(unlockedActiveQ unlockedActiveQueuer))
	underRLock(func(unlockedActiveQ unlockedActiveQueueReader))

	update(newPod *v1.Pod, oldPodInfo *framework.QueuedPodInfo) *framework.QueuedPodInfo
	delete(pInfo *framework.QueuedPodInfo) error
	pop(logger klog.Logger) (*framework.QueuedPodInfo, error)
	list() []*v1.Pod
	len() int
	has(pInfo *framework.QueuedPodInfo) bool

	listInFlightEvents() []interface{}
	listInFlightPods() []*v1.Pod
	clusterEventsForPod(logger klog.Logger, pInfo *framework.QueuedPodInfo) ([]*clusterEvent, error)
	addEventIfPodInFlight(oldPod, newPod *v1.Pod, event framework.ClusterEvent) bool
	addEventIfAnyInFlight(oldObj, newObj interface{}, event framework.ClusterEvent) bool

	schedulingCycle() int64
	done(pod types.UID)
	close()
	broadcast()
}

// unlockedActiveQueuer defines activeQ methods that are not protected by the lock itself. - unlockedActiveQueuer 定义了 activeQ 的方法，这些方法不是由锁本身保护的。
// underLock() method should be used to protect these methods. - underLock() 方法应该用于保护这些方法。
type unlockedActiveQueuer interface {
	unlockedActiveQueueReader
	AddOrUpdate(pInfo *framework.QueuedPodInfo)
}

// unlockedActiveQueueReader defines activeQ read-only methods that are not protected by the lock itself.
// underLock() or underRLock() method should be used to protect these methods. - underLock() 或 underRLock() 方法应该用于保护这些方法。
type unlockedActiveQueueReader interface {
	Get(pInfo *framework.QueuedPodInfo) (*framework.QueuedPodInfo, bool)
	Has(pInfo *framework.QueuedPodInfo) bool
}

// activeQueue implements activeQueuer. All of the fields have to be protected using the lock. - activeQueue 实现了 activeQueuer。所有字段必须使用锁来保护。
type activeQueue struct {
	// lock synchronizes all operations related to activeQ. - lock 同步所有与 activeQ 相关的操作。
	// It protects activeQ, inFlightPods, inFlightEvents, schedulingCycle and closed fields. - 它保护 activeQ、inFlightPods、inFlightEvents、schedulingCycle 和 closed 字段。
	// Caution: DO NOT take "SchedulingQueue.lock" after taking "lock". - 注意：在获取 "lock" 后不要获取 "SchedulingQueue.lock"。
	// You should always take "SchedulingQueue.lock" first, otherwise the queue could end up in deadlock. - 你应该总是先获取 "SchedulingQueue.lock"，否则队列可能会导致死锁。
	// "lock" should not be taken after taking "nLock". - "lock" 不应该在获取 "nLock" 后获取。
	// Correct locking order is: SchedulingQueue.lock > lock > nominator.nLock. - 正确的锁定顺序是：SchedulingQueue.lock > lock > nominator.nLock。
	lock sync.RWMutex

	// activeQ is heap structure that scheduler actively looks at to find pods to
	// schedule. Head of heap is the highest priority pod. - activeQ 是调度器主动查找要调度的 Pod 的堆结构。堆的头部是最高优先级的 Pod。
	queue *heap.Heap[*framework.QueuedPodInfo]

	// cond is a condition that is notified when the pod is added to activeQ. - cond 是一个条件，当 Pod 被添加到 activeQ 时会通知它。
	// It is used with lock. - 它与 lock 一起使用。
	cond sync.Cond

	// inFlightPods holds the UID of all pods which have been popped out for which Done
	// hasn't been called yet - in other words, all pods that are currently being
	// processed (being scheduled, in permit, or in the binding cycle). - “inFlightPods” 保存了所有已弹出但 “Done” 尚未被调用的 pod 的 UID。换句话说，所有正在处理（正在调度、在 permit 中或在绑定周期中）的 Pod。
	//
	// The values in the map are the entry of each pod in the inFlightEvents list. - 映射中的值是 inFlightEvents 列表中每个 Pod 的条目。
	// The value of that entry is the *v1.Pod at the time that scheduling of that
	// pod started, which can be useful for logging or debugging. - 该条目中的值是该 Pod 开始调度时的 *v1.Pod，这对于日志记录或调试非常有用。
	inFlightPods map[types.UID]*list.Element

	// inFlightEvents holds the events received by the scheduling queue
	// (entry value is clusterEvent) together with in-flight pods (entry
	// value is *v1.Pod). Entries get added at the end while the mutex is
	// locked, so they get serialized. - 在 Pop 中添加 Pod 条目，并用于跟踪在 Pod 调度尝试开始后发生的所有事件。
	//
	// The pod entries are added in Pop and used to track which events
	// occurred after the pod scheduling attempt for that pod started. - 在 Pop 中添加 Pod 条目，并用于跟踪在 Pod 调度尝试开始后发生的所有事件。
	// They get removed when the scheduling attempt is done, at which
	// point all events that occurred in the meantime are processed. - 当调度尝试完成时，这些条目会被删除，此时所有在调度尝试期间发生的事件都会被处理。
	//
	// After removal of a pod, events at the start of the list are no
	// longer needed because all of the other in-flight pods started
	// later. Those events can be removed. - 当调度尝试完成时，这些条目会被删除，此时所有在调度尝试期间发生的事件都会被处理。
	inFlightEvents *list.List

	// schedCycle represents sequence number of scheduling cycle and is incremented
	// when a pod is popped. - schedCycle 表示调度周期的序列号，当一个 Pod 被弹出时递增。
	schedCycle int64

	// closed indicates that the queue is closed. - closed 表示队列已关闭。
	// It is mainly used to let Pop() exit its control loop while waiting for an item. - 它主要用于让 Pop() 退出其控制循环，等待新项目。
	closed bool

	// isSchedulingQueueHintEnabled indicates whether the feature gate for the scheduling queue is enabled. - isSchedulingQueueHintEnabled 表示调度队列的特性门是否启用。
	isSchedulingQueueHintEnabled bool
}

func newActiveQueue(queue *heap.Heap[*framework.QueuedPodInfo], isSchedulingQueueHintEnabled bool) *activeQueue {
	aq := &activeQueue{
		queue:                        queue,
		inFlightPods:                 make(map[types.UID]*list.Element),
		inFlightEvents:               list.New(),
		isSchedulingQueueHintEnabled: isSchedulingQueueHintEnabled,
	}
	aq.cond.L = &aq.lock

	return aq
}

// underLock runs the fn function under the lock.Lock. - 在 lock.Lock 下运行 fn 函数。
// fn can run unlockedActiveQueuer methods but should NOT run any other activeQueue method,
// as it would end up in deadlock. - fn 可以运行 unlockedActiveQueuer 方法，但不能运行任何其他 activeQueue 方法，否则会导致死锁。
func (aq *activeQueue) underLock(fn func(unlockedActiveQ unlockedActiveQueuer)) {
	aq.lock.Lock()
	defer aq.lock.Unlock()
	fn(aq.queue)
}

// underLock runs the fn function under the lock.RLock. - 在 lock.RLock 下运行 fn 函数。
// fn can run unlockedActiveQueueReader methods but should NOT run any other activeQueue method,
// as it would end up in deadlock. - fn 可以运行 unlockedActiveQueueReader 方法，但不能运行任何其他 activeQueue 方法，否则会导致死锁。
func (aq *activeQueue) underRLock(fn func(unlockedActiveQ unlockedActiveQueueReader)) {
	aq.lock.RLock()
	defer aq.lock.RUnlock()
	fn(aq.queue)
}

// update updates the pod in activeQ if oldPodInfo is already in the queue. - 如果 oldPodInfo 已经在队列中，则更新队列中的 Pod。
// It returns new pod info if updated, nil otherwise. - 如果更新了，则返回新的 Pod 信息，否则返回 nil。
func (aq *activeQueue) update(newPod *v1.Pod, oldPodInfo *framework.QueuedPodInfo) *framework.QueuedPodInfo {
	aq.lock.Lock()
	defer aq.lock.Unlock()

	if pInfo, exists := aq.queue.Get(oldPodInfo); exists {
		_ = pInfo.Update(newPod)
		aq.queue.AddOrUpdate(pInfo)
		return pInfo
	}
	return nil
}

// delete deletes the pod info from activeQ. - 从队列中删除 Pod 信息。
func (aq *activeQueue) delete(pInfo *framework.QueuedPodInfo) error {
	aq.lock.Lock()
	defer aq.lock.Unlock()

	return aq.queue.Delete(pInfo)
}

// pop removes the head of the queue and returns it. - 从队列的头部移除并返回一个项目。
// It blocks if the queue is empty and waits until a new item is added to the queue. - 如果队列为空，它会阻塞，直到一个新的项目被添加到队列中。
// It increments scheduling cycle when a pod is popped. - 当一个 Pod 被弹出时，它会递增调度周期。
func (aq *activeQueue) pop(logger klog.Logger) (*framework.QueuedPodInfo, error) {
	aq.lock.Lock()
	defer aq.lock.Unlock()
	for aq.queue.Len() == 0 {
		// When the queue is empty, invocation of Pop() is blocked until new item is enqueued. - 当队列为空时，Pop() 的调用被阻塞，直到新的项目被入队。
		// When Close() is called, the p.closed is set and the condition is broadcast,
		// which causes this loop to continue and return from the Pop(). - 当 Close() 被调用时，p.closed 被设置，条件被广播，这会导致这个循环继续并从 Pop() 返回。
		if aq.closed {
			logger.V(2).Info("Scheduling queue is closed")
			return nil, nil
		}
		aq.cond.Wait()
	}
	pInfo, err := aq.queue.Pop()
	if err != nil {
		return nil, err
	}
	pInfo.Attempts++
	aq.schedCycle++
	// In flight, no concurrent events yet. - 在飞行中，没有并发事件。
	if aq.isSchedulingQueueHintEnabled {
		aq.inFlightPods[pInfo.Pod.UID] = aq.inFlightEvents.PushBack(pInfo.Pod)
	}

	// Update metrics and reset the set of unschedulable plugins for the next attempt. - 更新指标并重置未调度的插件集，为下一次尝试做准备。
	for plugin := range pInfo.UnschedulablePlugins.Union(pInfo.PendingPlugins) {
		metrics.UnschedulableReason(plugin, pInfo.Pod.Spec.SchedulerName).Dec()
	}
	pInfo.UnschedulablePlugins.Clear()
	pInfo.PendingPlugins.Clear()

	return pInfo, nil
}

// list returns all pods that are in the queue. - 返回队列中所有 Pod。
func (aq *activeQueue) list() []*v1.Pod {
	aq.lock.RLock()
	defer aq.lock.RUnlock()
	var result []*v1.Pod
	for _, pInfo := range aq.queue.List() {
		result = append(result, pInfo.Pod)
	}
	return result
}

// len returns length of the queue.
func (aq *activeQueue) len() int {
	return aq.queue.Len()
}

// has inform if pInfo exists in the queue. - 检查 pInfo 是否存在于队列中。
func (aq *activeQueue) has(pInfo *framework.QueuedPodInfo) bool {
	aq.lock.RLock()
	defer aq.lock.RUnlock()
	return aq.queue.Has(pInfo)
}

// listInFlightEvents returns all inFlightEvents. - 返回所有 inFlightEvents。
func (aq *activeQueue) listInFlightEvents() []interface{} {
	aq.lock.RLock()
	defer aq.lock.RUnlock()
	var values []interface{}
	for event := aq.inFlightEvents.Front(); event != nil; event = event.Next() {
		values = append(values, event.Value)
	}
	return values
}

// listInFlightPods returns all inFlightPods. - 返回所有 inFlightPods。
func (aq *activeQueue) listInFlightPods() []*v1.Pod {
	aq.lock.RLock()
	defer aq.lock.RUnlock()
	var pods []*v1.Pod
	for _, obj := range aq.inFlightPods {
		pods = append(pods, obj.Value.(*v1.Pod))
	}
	return pods
}

// clusterEventsForPod gets all cluster events that have happened during pod for pInfo is being scheduled. - 获取在 Pod 调度期间发生的所有集群事件。
func (aq *activeQueue) clusterEventsForPod(logger klog.Logger, pInfo *framework.QueuedPodInfo) ([]*clusterEvent, error) {
	aq.lock.RLock()
	defer aq.lock.RUnlock()
	logger.V(5).Info("Checking events for in-flight pod", "pod", klog.KObj(pInfo.Pod), "unschedulablePlugins", pInfo.UnschedulablePlugins, "inFlightEventsSize", aq.inFlightEvents.Len(), "inFlightPodsSize", len(aq.inFlightPods))

	// AddUnschedulableIfNotPresent is called with the Pod at the end of scheduling or binding. - AddUnschedulableIfNotPresent 在调度或绑定结束时调用 Pod。
	// So, given pInfo should have been Pop()ed before,
	// we can assume pInfo must be recorded in inFlightPods and thus inFlightEvents. - 我们可以假设 pInfo 必须记录在 inFlightPods 中，因此也在 inFlightEvents 中。
	inFlightPod, ok := aq.inFlightPods[pInfo.Pod.UID]
	if !ok {
		return nil, fmt.Errorf("in flight Pod isn't found in the scheduling queue. If you see this error log, it's likely a bug in the scheduler")
	}

	var events []*clusterEvent
	for event := inFlightPod.Next(); event != nil; event = event.Next() {
		e, ok := event.Value.(*clusterEvent)
		if !ok {
			// Must be another in-flight Pod (*v1.Pod). Can be ignored.
			continue
		}
		events = append(events, e)
	}
	return events, nil
}

// addEventIfPodInFlight adds clusterEvent to inFlightEvents if the newPod is in inFlightPods. - 如果 newPod 在 inFlightPods 中，则将 clusterEvent 添加到 inFlightEvents。
// It returns true if pushed the event to the inFlightEvents. - 如果将事件推送到 inFlightEvents，则返回 true。
func (aq *activeQueue) addEventIfPodInFlight(oldPod, newPod *v1.Pod, event framework.ClusterEvent) bool {
	aq.lock.Lock()
	defer aq.lock.Unlock()

	_, ok := aq.inFlightPods[newPod.UID]
	if ok {
		aq.inFlightEvents.PushBack(&clusterEvent{
			event:  event,
			oldObj: oldPod,
			newObj: newPod,
		})
	}
	return ok
}

// addEventIfAnyInFlight adds clusterEvent to inFlightEvents if any pod is in inFlightPods. - 如果任何 Pod 在 inFlightPods 中，则将 clusterEvent 添加到 inFlightEvents。
// It returns true if pushed the event to the inFlightEvents. - 如果将事件推送到 inFlightEvents，则返回 true。
func (aq *activeQueue) addEventIfAnyInFlight(oldObj, newObj interface{}, event framework.ClusterEvent) bool {
	aq.lock.Lock()
	defer aq.lock.Unlock()

	if len(aq.inFlightPods) != 0 {
		aq.inFlightEvents.PushBack(&clusterEvent{
			event:  event,
			oldObj: oldObj,
			newObj: newObj,
		})
		return true
	}
	return false
}

func (aq *activeQueue) schedulingCycle() int64 {
	aq.lock.RLock()
	defer aq.lock.RUnlock()
	return aq.schedCycle
}

// done must be called for pod returned by Pop. This allows the queue to
// keep track of which pods are currently being processed. - done 必须为 Pop 返回的 Pod 调用。这允许队列跟踪哪些 Pod 当前正在处理。
// 标记 Pod 调度已完成, 不要回队列 - (3)（k8s-scheduler-chain）
func (aq *activeQueue) done(pod types.UID) {
	aq.lock.Lock()
	defer aq.lock.Unlock()

	inFlightPod, ok := aq.inFlightPods[pod]
	if !ok {
		// This Pod is already done()ed. - 这个 Pod 已经 done()ed。
		return
	}
	delete(aq.inFlightPods, pod)

	// Remove the pod from the list. - 从列表中删除 Pod。
	aq.inFlightEvents.Remove(inFlightPod)

	// Remove events which are only referred to by this Pod
	// so that the inFlightEvents list doesn't grow infinitely. - 删除仅由这个 Pod 引用的所有事件，以防止 inFlightEvents 列表无限增长。
	// If the pod was at the head of the list, then all
	// events between it and the next pod are no longer needed
	// and can be removed. - 如果这个 Pod 在列表的头部，那么所有在它和下一个 Pod 之间的事件都不再需要，可以删除。
	for {
		e := aq.inFlightEvents.Front()
		if e == nil {
			// Empty list. - 空列表。
			break
		}
		if _, ok := e.Value.(*clusterEvent); !ok {
			// A pod, must stop pruning.
			break
		}
		aq.inFlightEvents.Remove(e)
	}
}

// close closes the activeQueue. - 关闭队列
func (aq *activeQueue) close() {
	aq.lock.Lock()
	aq.closed = true
	aq.lock.Unlock()
}

// broadcast notifies the pop() operation that new pod(s) was added to the activeQueue. - 通知 pop() 操作，新的 Pod 已添加到队列中。
func (aq *activeQueue) broadcast() {
	aq.cond.Broadcast()
}
