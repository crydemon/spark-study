package think;



import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class ThinkReflect {
}

class Person {

}

class Test {

    public static void main(String[] args) throws ClassNotFoundException {
        Class<Test> class1 = Test.class;
        System.out.println("类名1：" + class1.getName());

        Test Test = new Test();
        Class<? extends Test> class2 = Test.getClass();
        System.out.println("类名2：" + class2.getName());

        Class<?> class3 = Class.forName("Test");
        System.out.println("类名3：" + class3.getName());
        System.out.println(class1 == class2);
        System.out.println(class1 == class3);
    }
}

class TestConstructor {
    public static void main(String[] args) throws Exception {
        Class<?> personClass = Class.forName("Person");
        //获取所有的构造函数，包括私有的，不包括父类的
        Constructor<?>[] allConstructors = personClass.getDeclaredConstructors();
        //获取所有公有的构造函数，包括父类的
        Constructor<?>[] publicConstructors = personClass.getConstructors();
        System.out.println("遍历之后的构造函数：");
        for (Constructor c1 : allConstructors) {
            System.out.println(c1);
        }

        Constructor<?> c2 = personClass.getDeclaredConstructor(String.class, int.class, int.class);
        c2.setAccessible(true); //设置是否可访问，因为该构造器是private的，所以要手动设置允许访问，如果构造器是public的就不用设置
        Object person = c2.newInstance("刘俊重", 90, 193);   //使用反射创建Person类的对象,并传入参数
        System.out.println(person.toString());
    }
}

class TestField {
    public static void main(String[] args) throws Exception {
        Class<Person> personClass = Person.class;
        //获取所有的成员变量，包含私有的
        Field[] allFields = personClass.getDeclaredFields();
        //获取所有公有的成员变量，包含父类的
        Field[] publicFields = personClass.getFields();
        System.out.println("所有的成员变量：");
        for (Field f : allFields) {
            System.out.println(f);
        }
        //获取某个变量的值
        //创建对象的实例
        Constructor<Person> c = personClass.getDeclaredConstructor(String.class);
        c.setAccessible(true); //因为该构造函数时私有的，需要在这里设置成可访问的
        Person person = c.newInstance("刘俊重");
        //获取变量name对象
        Field field = personClass.getDeclaredField("name");
        field.setAccessible(true); //因为变量name是私有的，需要在这里设置成可访问的
        //注意对比下面这两行，官方对field.get(Object obj)方法的解释是返回对象obj字段field的值
        Object value = field.get(person);
        //String name = person.getName();
        System.out.println("获取的变量的值是：" + value);
    }
}


class TestMethod {
    public static void main(String[] args) throws Exception {
        Person person = new Person();
        Class<? extends Person> personClass = person.getClass();
        Method[] allMethods = personClass.getDeclaredMethods();
        Method[] publicMethods = personClass.getMethods();
        System.out.println("遍历所有的方法：");
        for (Method m : allMethods) {
            System.out.println(m);
        }

        //下面是测试通过反射调用函数
        //通过反射创建实例对象,默认调无参构造函数

        Person person2 = personClass.newInstance();
        //获取要调用的方法,要调用study方法，包含int和String参数，注意int和Integer在这有区别
        Method method = personClass.getMethod("think", String.class);
        method.setAccessible(true);
        Object o = method.invoke(person2, "java");
    }
}


