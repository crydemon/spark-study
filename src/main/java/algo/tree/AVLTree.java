package algo.tree;

//一棵AVL树是其每个结点的左子树和右子树的高度最多相差1的二叉查找树(空树的高度为-1)，
//这个差值也称为平衡因子（其取值可以是1，0，-1，平衡因子是某个结点左右子树层数的差值，
// 有的书上定义是左边减去右边，有的书上定义是右边减去左边，这样可能会有正负的区别，
// 但是这个并不影响我们对平衡二叉树的讨论）
public class AVLTree {
    private Node root;

    private class Node {
        private int key;
        private int balance;
        private int height;
        private Node left, right, parent;

        Node(int k, Node p) {
            key = k;
            parent = p;
        }
    }

    public boolean insert(int key) {
        //第一个节点作为root
        if (root == null) {
            root = new Node(key, null);
            return true;
        }
        Node n = root;
        Node parent;
        while (true) {
            if (n.key == key) {
                //所有的节点的key不同
                return false;
            }
            parent = n;
            boolean goLeft = n.key > key;
            n = goLeft ? n.left : n.right;
            //找到叶子节点
            if (n == null) {
                if (goLeft) {
                    parent.left = new Node(key, parent);
                } else {
                    parent.right = new Node(key, parent);
                }
                //谁的儿子谁负责
                reBalance(parent);
                break;
            }
        }
        return true;
    }

    private void reBalance(Node n) {
        setBalance(n);
        System.out.println("balance:" + n.balance);
        if (n.balance == -2) {
            if (height(n.left.left) >= height(n.left.right)) {
                n = rotateRight(n);
            } else {
                n = rotateLeftThenRight(n);
            }
        } else if (n.balance == 2) {
            if (height(n.right.right) > height(n.right.left)) {
                n = rotateLeft(n);
            } else {
                n = rotateRightThenLeft(n);
            }
        }
        if (n.parent != null) {
            System.out.println("n.parent: " + n.parent.key);
            reBalance(n.parent);
        } else {
            root = n;
        }
    }

    private Node rotateRightThenLeft(Node n) {
        return rotateLeft(rotateRight(n));
    }

    private Node rotateLeftThenRight(Node n) {
        return rotateRight(rotateLeft(n));
    }

    private Node rotateRight(Node a) {
        Node b = a.left;
        b.parent = a.parent;
        a.left = b.right;
        if (a.left != null) {
            a.left.parent = a;
        }
        b.right = a;

        return arrange(a, b);
    }

    private Node rotateLeft(Node a) {
        Node b = a.right;
        b.parent = a.parent;

        a.right = b.left;

        if (a.right != null)
            a.right.parent = a;

        b.left = a;


        return arrange(a, b);
    }

    private void delete(Node node) {
        //为叶子节点
        if (node.left == null && node.right == null) {
            //只有一个节点
            if (node.parent == null) {
                root = null;
            } else {
                Node parent = node.parent;
                if (parent.left == node) {
                    parent.left = null;
                } else {
                    parent.right = null;
                }
                reBalance(parent);
            }
            //左子树不为空
        } else if (node.left != null) {
            Node child = node.left;
            //找到左子树最大
            while (child.right != null) {
                child = child.right;
            }
            node.key = child.key;
            delete(child);
            //右子树不为空
        } else {
            Node child = node.right;
            //找到右子树最小
            while (child.left != null) {
                child = child.left;
            }
            node.key = child.key;
            delete(child);
        }
    }

    public void delete(int delKey) {
        if (root == null) return;
        Node node = root;
        Node child = root;
        while (child != null) {
            node = child;
            child = delKey >= node.key ? node.right : node.left;
            if (delKey == node.key) {
                delete(node);
                return;
            }
        }
    }

    private Node arrange(Node a, Node b) {
        System.out.println("arrange");
        a.parent = b;

        if (b.parent != null) {
            if (b.parent.right == a) {
                b.parent.right = b;
            } else {
                b.parent.left = b;
            }
        }
        setBalance(a, b);
        return b;
    }


    private int height(Node n) {
        return n == null ? -1 : n.height;
    }

    private void reHeight(Node n) {
        if (n == null) return;
        n.height = 1 + Math.max(height(n.left), height(n.right));
    }

    private void setBalance(Node... nodes) {
        for (Node n : nodes) {
            reHeight(n);
            n.balance = height(n.right) - height(n.left);
        }
    }


    public void printBalance() {
        printBalance(root);
    }

    //中序遍历
    private void printBalance(Node n) {
        if (n != null) {
            printBalance(n.left);
            //System.out.printf("%s ", n.balance);
            System.out.printf("%s, %s|", n.key, n.height);
            printBalance(n.right);
        }
    }

    public static void main(String[] args) {
        AVLTree tree = new AVLTree();

        System.out.println("Inserting values 1 to 100");
        for (int i = 1; i <= 5; i++) {
            System.out.println(i + "---------------------------");
            tree.insert(i);
        }


        System.out.print("Printing balance: ");
        tree.printBalance();
    }
}

class TestReference {
    static class Node {
        int key;
        Node next;

        public Node(int key) {
            this.key = key;
            this.next = null;
        }

        @Override
        public String toString() {
            if (this.next == null) return this.key + "";
            else return key + "->" + this.next.toString();
        }
    }

    public static void main(String[] args) {
        Node a = new Node(4);

        Node b = a.next;
        a.next = new Node(15);
        System.out.println(b == a.next);
        b = a.next;
        b.next = new Node(555);
        System.out.println(b == a.next);
        System.out.println(b.toString());
        System.out.println(a.toString());

    }
}









