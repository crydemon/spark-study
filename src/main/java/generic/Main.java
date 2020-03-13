package generic;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;

import static java.lang.Class.forName;

public class Main {
    public static void main(String[] args) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        ArrayList<Integer> list = new ArrayList<Integer>();
        list.add(111);
        list.add(222);
        Class clazz3 = forName("java.util.ArrayList");//获取ArrayList的字节码文件
        Method m = clazz3.getMethod("add", Object.class);//获取add() 方法，Object.class 代表任意对象类型的数据
        m.invoke(list,"Hello");//通过反射添加字符串类型元素数据
        System.out.println(list);//运行结果：[111, 222, Hello]
    }
}
