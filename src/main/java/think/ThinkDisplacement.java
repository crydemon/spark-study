package think;

public class ThinkDisplacement {

    public static void main(String[] args) {
        int cap = 19;//假设第i位出现第一个1,最高位为n, i != n
        int n = cap - 1;//则i-1 出现第一个1
        n |= n >>> 1; // 则i-1,i-2位同时为1。第n,n-1位同时为1
        n |= n >>> 2;// i-1,i-2,i-3,i-4同时为1。n,n-1,n-2,n-3同时为1。如此递推
        n |= n >>> 4;
        n |= n >>> 8;
        System.out.println(Integer.toBinaryString(n));
        System.out.println(Integer.toBinaryString(n>>>16));
        System.out.println("16");
        n |= n >>> 16;
        System.out.println(Integer.toBinaryString(n));
        n = (n < 0) ? 1 : n + 1;
        System.out.println(Integer.toBinaryString(n));
        //当i = n 时， 则cap 本身为 2 的n次幂
    }
}
