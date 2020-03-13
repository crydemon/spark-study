package generic;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GenericArrayWithTypeToken<T> {
    private T[] array;

    @SuppressWarnings("unchecked")
    public GenericArrayWithTypeToken(Class<T> type, int sz) {
        array = (T[]) Array.newInstance(type, sz);
    }

    public void put(int index, T item) {
        array[index] = item;
    }

    public T get(int index) {
        return array[index];
    }

    // Expose the underlying representation:
    public T[] rep() {
        return array;
    }

    static class A{
        int i = 0;
    }

    public static void main(String[] args) {
        List[] a = (List<Integer>[])new ArrayList[10];
        System.out.println(a[0].getClass().toString());
        System.out.println(Arrays.toString(a));
        GenericArrayWithTypeToken<ArrayList> gai = new GenericArrayWithTypeToken<>(ArrayList.class, 10);
        // This now works:
        ArrayList[] ia = gai.rep();
        System.out.println(ia.length);
        ia[0] = new ArrayList<Integer>(10);
        ia[0].add(5);
        System.out.println(Arrays.toString(ia));
    }
}