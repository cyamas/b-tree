package main

import (
	"errors"
)

const (
	maxKeys = 101
	minKeys = maxKeys / 2

	maxChildren = maxKeys + 1
)

type BTree struct {
	root *Node
}

func NewBTree() *BTree {
	return &BTree{root: newNode()}
}

func (b *BTree) Depth() int {
	depth := 1
	if b.root.isLeaf() {
		return depth
	}
	return b.root.Depth(1)
}

func (b *BTree) Insert(newKey int) {
	if b.root.isLeaf() {
		b.insertAtRoot(newKey)
		return
	}

	nextNode := b.root.nextNode(newKey)
	right, pushed := nextNode.insert(newKey)
	if pushed != 0 {
		if b.root.size() < maxKeys {
			b.root.addFromSplit(pushed, right)
		} else {
			b.addNewRootFromSplit(pushed, right)
		}
	}
}

func (b *BTree) insertAtRoot(newKey int) {
	if b.root.size() < maxKeys {
		insertIdx := b.root.indexForInsertion(newKey)
		b.root.addKey(newKey, insertIdx)
		return
	}
	b.addNewRoot(newKey)
}

func (b *BTree) Delete(key int) error {
	root := b.root
	idx, found := root.indexForDeletion(key)

	if found {
		b.deleteFromRoot(idx)
		return nil
	}

	nextChild := root.children[idx]
	childSize, err := nextChild.delete(key)
	if err != nil {
		return err
	}
	if childSize < minKeys {
		siblingIdx, parentKeyIdx := root.getSiblingIndex(idx), root.getParentKeyIndex(idx)
		sibling := root.children[siblingIdx]
		mergeRootSize := childSize + sibling.size() + root.size()
		switch {
		case sibling.size() > minKeys:
			root.swapKeys(idx, siblingIdx, parentKeyIdx)
		case root.size() == maxKeys || mergeRootSize > maxKeys:
			root.mergeChildren(idx, siblingIdx, parentKeyIdx)
		default:
			b.mergeRoot()
		}
	}

	return nil
}

func (b *BTree) deleteFromRoot(idx int) {
	b.root.removeKey(idx)
	if b.root.isLeaf() {
		return
	}
	leftChild := b.root.children[idx]
	rightChild := b.root.children[idx+1]
	switch {
	case b.root.size() == 0 && b.root.numChildren() == 1:
		b.root = b.root.children[0]
	case rightChild.size() > leftChild.size():
		successor, childSize := b.root.getRightSuccessor(idx + 1)
		insertIdx := b.root.indexForInsertion(successor)
		b.root.addKey(successor, insertIdx)
		if childSize < minKeys && b.root.size() > 0 {
			b.root.repairSubTree(idx, idx+1, idx)
		}
	default:
		successor, childSize := b.root.getLeftSuccessor(idx)
		insertIdx := b.root.indexForInsertion(successor)
		b.root.addKey(successor, insertIdx)
		if childSize < minKeys && b.root.size() > 0 {
			b.root.repairSubTree(idx+1, idx, idx)
		}
	}
}

func (*Node) getParentKeyIndex(idx int) int {
	if idx == 0 {
		return 0
	}
	return idx - 1
}

func (n *Node) getSiblingIndex(idx int) int {
	if idx == 0 {
		return 1
	}
	return idx - 1
}

func (n *Node) swapKeys(addToIdx, takeFromIdx, keyIdx int) {
	addTo := n.children[addToIdx]
	takeFrom := n.children[takeFromIdx]

	if takeFromIdx < addToIdx {
		n.takeFromLeftSibling(addTo, takeFrom, keyIdx)
	} else {
		n.takeFromRightSibling(addTo, takeFrom, keyIdx)
	}
}

func (n *Node) takeFromLeftSibling(addTo, takeFrom *Node, keyIdx int) {
	takeFromSize := takeFrom.size()
	takeKey := takeFrom.keys[takeFromSize-1]
	takeFrom.keys[takeFrom.size()-1] = 0
	insertIdx := addTo.indexForInsertion(n.keys[keyIdx])
	addTo.addKey(n.keys[keyIdx], insertIdx)
	n.keys[keyIdx] = takeKey

	if !takeFrom.isLeaf() {
		rightGrandchild := takeFrom.children[takeFrom.numChildren()-1]
		takeFrom.children[takeFrom.numChildren()-1] = nil
		addTo.shiftChildrenRight(0)
		addTo.children[0] = rightGrandchild
	}
}

func (n *Node) takeFromRightSibling(addTo, takeFrom *Node, keyIdx int) {
	takeKey := takeFrom.keys[0]
	takeFrom.removeKey(0)
	insertIdx := addTo.indexForInsertion(n.keys[keyIdx])
	addTo.addKey(n.keys[keyIdx], insertIdx)
	n.keys[keyIdx] = takeKey

	if !takeFrom.isLeaf() {
		leftGrandchild := takeFrom.children[0]
		takeFrom.removeChild(0)
		addTo.children[addTo.numChildren()] = leftGrandchild
	}
}

func (b *BTree) mergeRoot() {
	root := b.root
	var grandchildren []*Node
	for i := range root.size() + 1 {
		child := root.children[i]
		if !child.isLeaf() {
			for j := range child.size() + 1 {
				grandchildren = append(grandchildren, child.children[j])
			}
		}
		for k := range child.size() {
			key := child.keys[k]
			insertIdx := root.indexForInsertion(key)
			root.addKey(key, insertIdx)
		}
		root.children[i] = nil
	}
	for m, node := range grandchildren {
		root.children[m] = node
	}
}

func (b *BTree) getRightmostLeafDepth(node *Node, depth int) int {
	if node.isLeaf() {
		return depth
	}
	return b.getRightmostLeafDepth(node.children[node.size()], depth+1)
}

func (b *BTree) keyExists(key int, node *Node) bool {
	size := node.size()
	for i := range size {
		switch {
		case key == node.keys[i]:
			return true
		case key < node.keys[i] && !node.isLeaf():
			return b.keyExists(key, node.children[i])
		}
	}
	if node.isLeaf() {
		return false
	}
	return b.keyExists(key, node.children[size])
}

func (b *BTree) addNewRoot(newKey int) {
	right, pushed := b.root.split(newKey)
	newRoot := newNode()
	newRoot.keys[0] = pushed
	newRoot.children[0] = b.root
	newRoot.children[1] = right
	b.root = newRoot
}

func (b *BTree) addNewRootFromSplit(prevPushed int, prevRight *Node) {
	right, newRootKey := b.root.split(prevPushed)
	newRoot := newNode()
	newRoot.keys[0] = newRootKey

	switch {
	case newRootKey < prevPushed:
		idx := right.indexForInsertion(prevPushed)
		right.shiftChildrenRight(idx)
		right.children[idx] = prevRight
	case newRootKey > prevPushed:
		idx := b.root.indexForInsertion(prevPushed)
		b.root.shiftChildrenRight(idx)
		b.root.children[idx] = prevRight
	default:
		right.shiftChildrenRight(0)
		right.children[0] = prevRight
	}
	newRoot.children[0] = b.root
	newRoot.children[1] = right
	b.root = newRoot
}

type Node struct {
	keys     [maxKeys]int
	children [maxKeys + 1]*Node
}

func newNode() *Node {
	return &Node{}
}

func (n *Node) size() int {
	for i, key := range n.keys {
		if key == 0 {
			return i
		}
	}
	return maxKeys
}

func (n *Node) numChildren() int {
	count := 0
	for _, child := range n.children {
		if child == nil {
			break
		}
		count++
	}
	return count
}

func (n *Node) Depth(treeDepth int) int {
	if n.isLeaf() {
		return treeDepth
	}
	return n.children[0].Depth(treeDepth + 1)
}

func (n *Node) nextNode(newKey int) *Node {
	idx := n.indexForInsertion(newKey)
	return n.children[idx]
}

func (n *Node) insert(newKey int) (*Node, int) {
	if n.isLeaf() {
		return n.insertAtLeafNode(newKey)
	}
	nextNode := n.nextNode(newKey)
	right, pushed := nextNode.insert(newKey)
	if pushed != 0 {
		if n.size() < maxKeys {
			n.addFromSplit(pushed, right)
		} else {
			return n.splitFromSplit(pushed, right)
		}
	}
	return nil, 0
}

func (n *Node) insertAtLeafNode(newKey int) (*Node, int) {
	if n.size() < maxKeys {
		idx := n.indexForInsertion(newKey)
		n.addKey(newKey, idx)
		return nil, 0
	}

	right, pushed := n.split(newKey)
	return right, pushed
}

func (n *Node) delete(key int) (int, error) {
	if n.isLeaf() {
		return n.deleteLeafKey(key)
	}
	idx, found := n.indexForDeletion(key)
	if found {
		return n.deleteNonLeafKey(idx), nil
	}
	nextChildSize, err := n.children[idx].delete(key)
	if err != nil {
		return -1, err
	}
	if nextChildSize >= minKeys {
		return n.size(), nil
	}
	smallerChildIdx := idx
	smallerChild := n.children[smallerChildIdx]
	parentKeyIdx, largerChildIdx := n.getParentKeyIndex(idx), n.getSiblingIndex(idx)
	largerChild := n.children[largerChildIdx]
	if smallerChild.size()+largerChild.size() < maxKeys {
		n.mergeChildren(smallerChildIdx, largerChildIdx, parentKeyIdx)
	} else {
		n.swapKeys(smallerChildIdx, largerChildIdx, parentKeyIdx)
	}
	return n.size(), nil
}

func (n *Node) deleteNonLeafKey(idx int) int {
	n.removeKey(idx)
	leftChild := n.children[idx]
	leftSize := leftChild.size()
	rightChild := n.children[idx+1]
	rightSize := rightChild.size()

	switch {
	case leftSize < rightSize:
		successor, childSize := n.getRightSuccessor(idx + 1)
		insertIdx := n.indexForInsertion(successor)
		n.addKey(successor, insertIdx)
		if childSize < minKeys {
			n.repairSubTree(idx, idx+1, idx)
		}
	case leftChild.isLeaf() && leftSize == minKeys:
		n.combineLeftAndRightChildren(idx, idx+1)
	default:
		successor, childSize := n.getLeftSuccessor(idx)
		insertIdx := n.indexForInsertion(successor)
		n.addKey(successor, insertIdx)
		if childSize < minKeys {
			n.repairSubTree(idx+1, idx, idx)
		}
	}
	return n.size()
}

func (n *Node) removeKey(idx int) {
	prevSize := n.size()
	n.keys[idx] = 0
	for i := idx; i < prevSize-1; i++ {
		n.keys[i], n.keys[i+1] = n.keys[i+1], n.keys[i]
	}
}

func (n *Node) getLeftSuccessor(targetChildIdx int) (int, int) {
	targetChild := n.children[targetChildIdx]

	if targetChild.isLeaf() {
		successor := targetChild.keys[targetChild.size()-1]
		targetChild.deleteLeafKey(successor)
		return successor, targetChild.size()
	}

	successor, childSize := targetChild.getLeftSuccessor(targetChild.numChildren() - 1)
	if childSize < minKeys {
		leftChildIdx, rightChildIdx := targetChild.numChildren()-2, targetChild.numChildren()-1
		targetChild.repairSubTree(leftChildIdx, rightChildIdx, targetChild.size()-1)
	}

	return successor, targetChild.size()
}

func (n *Node) getRightSuccessor(targetChildIdx int) (int, int) {
	targetChild := n.children[targetChildIdx]

	if targetChild.isLeaf() {
		successor := targetChild.keys[0]
		targetChild.removeKey(0)
		return successor, targetChild.size()
	}

	successor, childSize := targetChild.getRightSuccessor(0)
	if childSize < minKeys {
		leftChildIdx, rightChildIdx := 0, 1
		targetChild.repairSubTree(rightChildIdx, leftChildIdx, 0)
	}
	return successor, targetChild.size()
}

func (n *Node) repairSubTree(smallIdx, largeIdx, parentKeyIdx int) {
	small := n.children[smallIdx]
	large := n.children[largeIdx]

	if small.size()+large.size() >= maxKeys {
		n.swapKeys(largeIdx, smallIdx, parentKeyIdx)
		return
	}
	n.mergeChildren(smallIdx, largeIdx, parentKeyIdx)
}

func (n *Node) combineLeftAndRightChildren(leftIdx, rightIdx int) {
	left := n.children[leftIdx]
	right := n.children[rightIdx]
	for i := range left.size() {
		right.shiftKeysRight(i)
		right.keys[i] = left.keys[i]
	}
	if !left.isLeaf() {
		if right.size() == maxKeys {
			lastLeftChild := left.children[left.size()]
			firstRightChild := right.children[0]
			for j := range lastLeftChild.size() {
				key := lastLeftChild.keys[j]
				insertIdx := lastLeftChild.indexForInsertion(key)
				firstRightChild.addKey(key, insertIdx)
				//firstRightChild.AddKey(lastLeftChild.keys[j])
			}
			left.children[left.size()] = nil
		}
		for k := range left.numChildren() {
			right.shiftChildrenRight(k)
			right.children[k] = left.children[k]
		}
	}
	n.removeChild(leftIdx)
}

func (n *Node) mergeChildren(consumedChildIdx, targetChildIdx, parentKeyIdx int) {
	target := n.children[targetChildIdx]
	consumed := n.children[consumedChildIdx]

	n.mergeChildKeys(consumed, target)

	if !consumed.isLeaf() {
		n.mergeGrandchildren(consumedChildIdx, targetChildIdx)
	}

	n.removeChild(consumedChildIdx)

	insertIdx := target.indexForInsertion(n.keys[parentKeyIdx])
	target.addKey(n.keys[parentKeyIdx], insertIdx)
	n.removeKey(parentKeyIdx)
}

func (n *Node) mergeChildKeys(consumed, target *Node) {
	for i := range consumed.size() {
		key := consumed.keys[i]
		idx := target.indexForInsertion(key)
		target.addKey(key, idx)
	}
}

func (n *Node) mergeGrandchildren(consumedChildIdx, targetChildIdx int) {
	target := n.children[targetChildIdx]
	consumed := n.children[consumedChildIdx]
	offset := 0
	if consumedChildIdx >= targetChildIdx {
		offset += target.numChildren()
	}

	for i := range consumed.numChildren() {
		target.addChild(consumed.children[i], i+offset)
	}
}

func (n *Node) splitFromSplit(prevPushed int, prevRight *Node) (*Node, int) {
	right, newPushed := n.split(prevPushed)

	if newPushed == prevPushed {
		right.addChild(prevRight, 0)
	} else {
		idx := right.indexForInsertion(prevPushed)
		right.addChild(prevRight, idx)
	}
	return right, newPushed
}

func (n *Node) addFromSplit(newKey int, newChild *Node) {
	insertIdx := n.indexForInsertion(newKey)
	n.addKey(newKey, insertIdx)
	n.addChild(newChild, insertIdx+1)
}

func (n *Node) isLeaf() bool {
	return n.children[0] == nil
}

func (n *Node) addChild(node *Node, idx int) {
	n.shiftChildrenRight(idx)
	n.children[idx] = node
}

func (n *Node) addKey(key, idx int) {
	n.shiftKeysRight(idx)
	n.keys[idx] = key
}

func (n *Node) indexForInsertion(key int) int {
	left, right := 0, n.size()-1
	for left <= right {
		mid := (left + right) / 2
		switch {
		case mid == 0 && key < n.keys[mid]:
			return 0
		case mid == n.size()-1 && key > n.keys[mid]:
			return mid + 1
		case key < n.keys[mid] && key > n.keys[mid-1]:
			return mid
		default:
			if key < n.keys[mid] {
				right = mid - 1
				continue
			}
			left = mid + 1
		}
	}
	return left
}

func (n *Node) deleteLeafKey(key int) (int, error) {
	idx, _ := n.indexForDeletion(key)
	if idx < 0 {
		return idx, errors.New("key does not exist")
	}
	n.removeKey(idx)
	return n.size(), nil
}

func (n *Node) indexForDeletion(key int) (int, bool) {
	left, right := 0, n.size()-1
	for left <= right {
		mid := (left + right) / 2
		switch {
		case n.keys[mid] == key:
			return mid, true
		case n.keys[mid] < key:
			left = mid + 1
		default:
			right = mid - 1
		}
	}
	return left, false
}

func (n *Node) shiftKeysRight(idx int) {
	for i := maxKeys - 1; i > idx; i-- {
		n.keys[i] = n.keys[i-1]
	}
}

func (n *Node) removeChild(idx int) {
	prevSize := n.numChildren()
	n.children[idx] = nil
	for i := idx; i < prevSize; i++ {
		if n.children[i+1] == nil {
			break
		}
		n.children[i], n.children[i+1] = n.children[i+1], n.children[i]
	}
}

func (n *Node) shiftChildrenRight(idx int) {
	for i := maxKeys; i > idx; i-- {
		n.children[i] = n.children[i-1]
	}
}

func (n *Node) split(newKey int) (*Node, int) {
	right := newNode()
	pushed := 0
	insertIdx := n.indexForInsertion(newKey)
	switch {
	case insertIdx < minKeys:
		n.transferKeys(right, minKeys)
		n.addKey(newKey, insertIdx)
		pushed = n.extractKey(minKeys)
		if !n.isLeaf() {
			n.transferChildren(right, minKeys+1, -1)
		}
	case insertIdx > minKeys:
		pushed = n.extractKey(minKeys)
		n.transferKeys(right, minKeys)
		rightIdx := right.indexForInsertion(newKey)
		right.addKey(newKey, rightIdx)
		if !n.isLeaf() {
			n.transferChildren(right, minKeys+1, 0)
		}
	default:
		n.transferKeys(right, minKeys)
		pushed = newKey
		if !n.isLeaf() {
			n.transferChildren(right, minKeys+1, 0)
		}
	}
	return right, pushed
}

func (n *Node) extractKey(idx int) int {
	pushed := n.keys[idx]
	n.removeKey(idx)
	return pushed
}

func (n *Node) transferKeys(target *Node, idx int) {
	targetIdx := 0
	for i := idx; i < maxKeys; i++ {
		target.keys[targetIdx] = n.keys[i]
		n.keys[i] = 0
		targetIdx++
	}
}

func (n *Node) transferChildren(target *Node, startIdx, offset int) {
	targetIdx := 0

	for i := startIdx; i < maxChildren; i++ {
		childIdx := i + offset
		target.children[targetIdx] = n.children[childIdx]
		n.children[childIdx] = nil
		targetIdx++
	}
}
