package main

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
)

func TestUpdateStream(t *testing.T) {
	numKeys := 20000
	tree := createAscendingTestTree(numKeys)
	deleteKeys, insertKeys := createRandomStream(numKeys)
	for i := range numKeys {
		coin := rand.Float64()
		if coin < 0.5 {
			tree.Delete(deleteKeys[i])
			if !treeIsValid(tree) {
				t.Fatalf("Invalid Tree. Error while deleting key: %d", deleteKeys[i])
			}
		} else {
			tree.Insert(insertKeys[i])
			if !treeIsValid(tree) {
				t.Fatalf("Invalid Tree. Error while inserting key: %d", insertKeys[i])
			}
		}
	}
}

func createRandomStream(numKeys int) ([]int, []int) {
	deleteKeys := []int{}
	insertKeys := []int{}

	for i := 1; i <= numKeys; i++ {
		key := i * 10
		deleteKeys = append(deleteKeys, key)
	}
	for _, key := range deleteKeys {
		insertKeys = append(insertKeys, key+5)
	}

	rand.Shuffle(len(deleteKeys), func(i, j int) {
		deleteKeys[i], deleteKeys[j] = deleteKeys[j], deleteKeys[i]
	})
	rand.Shuffle(len(insertKeys), func(i, j int) {
		insertKeys[i], insertKeys[j] = insertKeys[j], insertKeys[i]
	})
	return deleteKeys, insertKeys
}

func TestDeleteKey(t *testing.T) {
	// Tests deletion of root node key where root node is leaf
	tree1 := createAscendingTestTree(4)
	// Tests deletion of left leaf key where: left leaf size < minKeys < right leaf size
	tree2 := createAscendingTestTree(6)
	// Tests deletion of leaf key where all leaves are <= minKeys and thus a merge must happen
	tree3 := createAscendingTestTree(5)
	// Tests deletion of leaf key where leaf size > minKeys
	tree4 := createAscendingTestTree(6)
	// Tests deletion of leaf key from depth 3 tree such that the depth must change to 2
	tree5 := createAscendingTestTree(17)
	// Tests deletion of non-leaf key such that it must merge it's left and right children
	tree6 := createAscendingTestTree(20)
	// Tests deletion of non-leaf key such that it must pull the last key from it's left child
	tree7 := createAscendingTestTree(20)
	tree7.Insert(105)
	tree7.Insert(115)
	// Tests deletion of non-leaf key such that it must pull the first key from it's right child
	tree8 := createAscendingTestTree(20)
	tree8.Insert(135)
	tree8.Insert(138)
	// Tests deletion of non-leaf key such that it must merge the left and right child and reduce the depth of the tree to 2
	tree9 := createAscendingTestTree(19)
	// Tests deletion of non-leaf key such that it must pull from either the left Child or the right Child
	tree10 := createAscendingTestTree(19)
	tree10.Insert(105)
	tree10.Insert(135)
	// Tests deletion of non-leaf key at depth 2 with tree depth of 4
	tree11 := createAscendingTestTree(53)
	// Tests deletion of non-leaf key at depth 3 with tree depth of 4
	tree12 := createAscendingTestTree(53)
	// Tests deletion of leaf key with tree depth of 4
	tree13 := createAscendingTestTree(53)
	// Tests consecutive non-leaf key deletions with starting tree depth of 4
	tree14 := createAscendingTestTree(53)
	tree14.Delete(180)
	// Same as test 14 but performs deletions on right side of root node
	tree15 := createAscendingTestTree(53)
	tree15.Delete(360)
	tree16 := createAscendingTestTree(53)
	tree16.Delete(180)
	tree16.Insert(180)
	// Tests Deletion from trees not created in Ascending Order
	tree17 := createAscendingTestTree(14)
	tree18 := createMidpointTestTree(76)
	// Tests deletion of root key of tree of depth 4
	tree19 := createAscendingTestTree(53)
	tree20 := createAscendingTestTree(63)

	tests := []struct {
		tree *BTree
		key  int
	}{
		{tree1, 30},
		{tree2, 20},
		{tree3, 10},
		{tree4, 50},
		{tree5, 170},
		{tree6, 120},
		{tree7, 120},
		{tree8, 120},
		{tree9, 120},
		{tree10, 120},
		{tree11, 180},
		{tree12, 60},
		{tree13, 520},
		{tree14, 60},
		{tree15, 330},
		{tree16, 180},
		{tree17, 10},
		{tree18, 500},
		{tree19, 270},
		{tree20, 270},
	}
	for _, tt := range tests {
		err := tt.tree.Delete(tt.key)
		if err != nil {
			if err.Error() != "key does not exist" {
				t.Fatalf("Error should say key does not exist")
			}
		}
		if tt.tree.keyExists(tt.key, tt.tree.root) {
			t.Fatalf("Key %d is still in the tree", tt.key)
		}
		if !treeIsValid(tt.tree) {
			t.Fatalf("Invalid tree")
		}
	}
}

func TestInsertKey(t *testing.T) {
	tree1 := createAscendingTestTree(4)
	tree2 := createAscendingTestTree(10)
	tree3 := createAscendingTestTree(16)
	tree4 := createAscendingTestTree(52)
	tree5 := createDescendingTestTree(52)
	tree6 := createMidpointTestTree(52)

	tests := []struct {
		tree *BTree
		key  int
	}{
		{tree1, 50},
		{tree2, 110},
		{tree3, 170},
		{tree4, 530},
		{tree5, 5},
		{tree6, 265},
	}

	for _, tt := range tests {
		tt.tree.Insert(tt.key)
		if !treeIsValid(tt.tree) {
			t.Fatalf("Invalid tree")
		}
	}
}

func TestTreeIsValid(t *testing.T) {
	tree1 := createAscendingTestTree(4)
	tree2 := createDescendingTestTree(5)
	tree3 := createMidpointTestTree(6)
	tree4 := createAscendingTestTree(59)

	tree7 := createRandomInsertionTestTree(59)
	tests := []struct {
		tree     *BTree
		expected bool
	}{
		{tree1, true},
		{tree2, true},
		{tree3, true},
		{tree4, true},
		{tree7, true},
	}

	count := 1
	for _, tt := range tests {
		count++
		isValid := treeIsValid(tt.tree)
		if isValid != tt.expected {
			t.Fatalf("Tree should be %t", tt.expected)
		}
	}
}

func TestKeyExists(t *testing.T) {
	tree1 := createAscendingTestTree(52)
	tree1.Insert(26)
	tree2 := createDescendingTestTree(52)
	tree2.Insert(452)
	tree3 := createMidpointTestTree(52)
	tree3.Insert(173)

	tests := []struct {
		tree     *BTree
		key      int
		expected bool
	}{
		{tree1, 27, false},
		{tree1, 25, false},
		{tree1, 20, true},
		{tree1, 26, true},
		{tree2, 450, false},
		{tree2, 26, false},
		{tree2, 453, false},
		{tree2, 452, true},
		{tree3, 170, true},
		{tree3, 172, false},
		{tree3, 174, false},
		{tree3, 173, true},
	}

	for _, tt := range tests {
		if tt.tree.keyExists(tt.key, tt.tree.root) != tt.expected {
			t.Fatalf("Key exists should be %t", tt.expected)
		}
	}
}

func TestTreeDepth(t *testing.T) {
	tree1 := NewBTree()
	tree1.root.children[0] = newNode()

	tree2 := NewBTree()
	tests := []struct {
		tree     *BTree
		expected int
	}{
		{tree1, 2},
		{tree2, 1},
	}

	for _, tt := range tests {
		depth := tt.tree.Depth()
		if depth != tt.expected {
			t.Fatalf("Tree should have depth %d. Got %d", tt.expected, depth)
		}
	}
}

func allTreeKeys(tree *BTree) [][maxKeys]int {
	result := [][maxKeys]int{}
	result = append(result, tree.root.keys)
	fmt.Println(tree.root.keys)
	depth := 1
	if !tree.root.isLeaf() {
		result = addChildrenToQueue(tree.root, result, depth)
	}
	return result
}

func addChildrenToQueue(node *Node, queue [][maxKeys]int, depth int) [][maxKeys]int {
	for i := range node.size() + 1 {
		child := node.children[i]
		queue = append(queue, child.keys)
		spacing := strings.Repeat("    ", depth)
		fmt.Println(spacing, child.keys)
		if !child.isLeaf() {
			queue = addChildrenToQueue(child, queue, depth+1)
		}
	}
	return queue
}

func treeIsValid(tree *BTree) bool {
	if tree.root.size() == 0 {
		return true
	}
	for i := range tree.root.size() {
		if tree.root.keys[i] == 0 {
			fmt.Println("tree.root.keys[i] == 0")
			return false
		}
		if i != 0 {
			if tree.root.keys[i] < tree.root.keys[i-1] {
				fmt.Println("tree.root.keys[i] < tree.root.keys[i-1]")
				return false
			}
		}
		if !tree.root.isLeaf() {
			if tree.root.size()+1 != tree.root.numChildren() {
				fmt.Println("tree.root.size() + 1 != tree.root.numChildren()")
				return false
			}
			if tree.root.children[i] == nil || tree.root.children[i+1] == nil {
				fmt.Println("tree.root.children[i] == nil || tree.root.children[i+1] == nil")
				return false
			}
			if ok := checkLeftChild(tree.root.children[i], tree.root.keys[i]); !ok {
				return false
			}
			if i == tree.root.size()-1 {
				if ok := checkRightChild(tree.root.children[i+1], tree.root.keys[i]); !ok {
					return false
				}
			}
		}
	}
	if !treeIsBalanced(tree) {
		return false
	}
	return true
}

func treeIsBalanced(tree *BTree) bool {
	root := tree.root
	if root.isLeaf() {
		return true
	}
	depth := tree.getRightmostLeafDepth(root, 1)
	for i := range root.size() + 1 {
		if ok := verifyDepth(root.children[i], 2, depth); !ok {
			return false
		}
	}
	return true
}

func verifyDepth(node *Node, nodeDepth, depth int) bool {
	if node.isLeaf() {
		if nodeDepth != depth {
			return false
		}
		return true
	}
	for i := range node.size() + 1 {
		if ok := verifyDepth(node.children[i], nodeDepth+1, depth); !ok {
			return false
		}
	}
	return true
}

func checkLeftChild(left *Node, parentKey int) bool {
	size := left.size()
	if size < maxKeys/2 {
		return false
	}
	if !left.isLeaf() && left.numChildren() != size+1 {
		for _, child := range left.children {
			if child == nil {
				break
			}
		}
		return false
	}
	for i := range size {
		if left.keys[i] > parentKey {
			fmt.Println("left.keys[i] > parentKey")
			fmt.Println("left.keys[i]:", left.keys[i])
			fmt.Println("parent key:", parentKey)
			return false
		}
		if i != 0 {
			if left.keys[i] < left.keys[i-1] {
				fmt.Println("if left.keys[i] < left.keys[i-1] {")
				fmt.Println("left.keys[i] < parentKey")
				fmt.Println("left.keys[i]: ", left.keys[i])
				fmt.Println("parentKey: ", parentKey)
				return false
			}
		}
		if !left.isLeaf() {
			if left.children[i] == nil || left.children[i+1] == nil {
				fmt.Println("if left.children[i] == nil || left.children[i+1] == nil {")
				return false
			}
			if ok := checkLeftChild(left.children[i], left.keys[i]); !ok {
				return false
			}
			if i == size-1 {
				return checkRightChild(left.children[i+1], left.keys[i])
			}
		}
	}
	return true
}

func checkRightChild(right *Node, parentKey int) bool {
	size := right.size()
	if size < maxKeys/2 {
		return false
	}
	if !right.isLeaf() && size+1 != right.numChildren() {
		for _, child := range right.children {
			if child == nil {
				break
			}
		}
		fmt.Println("right.size +1 != right.numchildren()")
		return false
	}
	for i := range size {
		if right.keys[i] < parentKey {
			fmt.Println("right.keys[i] < parentKey")
			fmt.Println("right.keys[i]: ", right.keys[i])
			fmt.Println("parentKey: ", parentKey)
			return false
		}
		if i != 0 {
			if right.keys[i] < right.keys[i-1] {
				fmt.Println("if right.keys[i] < right.keys[i-1] {")
				return false
			}
		}
		if !right.isLeaf() {
			leftGC := right.children[0]
			for j := range leftGC.size() {
				if leftGC.keys[j] < parentKey {
					fmt.Println("if leftGC.keys[j] < parentKey {")
					return false
				}
			}
			if right.children[i] == nil || right.children[i+1] == nil {
				fmt.Println("if right.children[i] == nil || right.children[i+1] == nil {")
				return false
			}
			if ok := checkLeftChild(right.children[i], right.keys[i]); !ok {
				fmt.Println("if ok := checkLeftChild(right.children[i], right.keys[i]); !ok {")
				return false
			}
			if i == size-1 {
				return checkRightChild(right.children[i+1], right.keys[i])
			}
		}
	}
	return true
}

func createAscendingTestTree(numKeys int) *BTree {
	tree := NewBTree()
	for i := 1; i <= numKeys; i++ {
		key := i * 10
		tree.Insert(key)
	}
	return tree
}

func createDescendingTestTree(numKeys int) *BTree {
	tree := NewBTree()
	for i := numKeys; i > 0; i-- {
		key := i * 100
		tree.Insert(key)
	}
	return tree
}

func createMidpointTestTree(numKeys int) *BTree {
	mini := 10
	maxi := numKeys * 10
	mid := numKeys / 2
	if numKeys%2 != 0 {
		mid++
	}
	tree := NewBTree()
	tree.Insert(mini)
	tree.Insert(maxi)
	for i := 1; i < mid; i++ {
		tree.Insert(mini + (i * 10))
		tree.Insert(maxi - (i * 10))
	}
	return tree
}

func createRandomInsertionTestTree(numKeys int) *BTree {
	tree := NewBTree()
	var keys []int
	for i := 1; i <= numKeys; i++ {
		keys = append(keys, i*100)
	}
	rand.Shuffle(numKeys, func(i, j int) {
	})
	for _, key := range keys {
		tree.Insert(key)
	}
	return tree
}
