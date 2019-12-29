package algo.tree;

import java.util.stream.IntStream;

public class BST {

    class Node {
        public int data;
        public Node left;
        public Node right;

        public Node(int val) {
            data = val;
            left = null;
            right = null;
        }
    }

    private Node root;

    public BST() {
        root = null;
    }

    public Node get(int val) {
        return get(root, val);
    }

    private Node get(Node x, int val) {
        if (x == null) return null;
        else if (x.data < val) return get(x.right, val);
        else if (x.data > val) return get(x.left, val);
        else return x;
    }

    public void put(int val) {
        Node newNode = new Node(val);
        if (root == null) {
            root = newNode;
            return;
        }
        Node current = root;
        while (current != null) {
            if (current.data > val) {
                if (current.left == null) {
                    current.left = newNode;

                    return;
                } else {
                    current = current.left;
                }
            } else {
                if (current.right == null) {
                    current.right = newNode;

                    return;
                } else {
                    current = current.right;
                }
            }
        }
    }

    public void delete(int val) {
        root = delete(root, val);
    }

    //会不会有内存泄露的问题
    private Node delete(Node x, int val) {
        if (x == null) return null;
        else if (x.data < val) x.right = delete(x.right, val);
        else if (x.data > val) x.left = delete(x.left, val);
        else {
            if (x.left == null) return x.right;
            else if (x.right == null) return x.left;
            else {
                x.data = minVal(x.right);
                x = delete(x.right, x.data);
            }
        }
        return x;
    }

    private int minVal(Node x) {
        if (x.left == null) return x.data;
        return minVal(x.left);
    }

    public boolean isBST(Node x) {
        if (x == null) return true;
        else if (x.left != null && x.left.data > x.data) {
            return false;
        } else if (x.right != null && x.right.data < x.data) {
            return false;
        } else return isBST(x.left) && isBST(x.right);
    }


    public static void main(String[] args) {
        BST tree = new BST();
        IntStream.range(0, 100000000).forEach(x -> {
            tree.put(x);
            if (x > 10) {
                tree.delete(x);
            }
            //vm option -ea
            assert tree.isBST(tree.root) : "not bst";
        });
    }
}
