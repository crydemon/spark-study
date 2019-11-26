import sun.misc.Unsafe;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;

public class CreateObjectMethod {

}


class CloneTest implements Cloneable {

    private String name;

    private int age;

    public String getName() {

        return name;

    }

    public void setName(String name) {

        this.name = name;

    }

    public int getAge() {

        return age;

    }

    public void setAge(int age) {

        this.age = age;

    }

    public CloneTest(String name, int age) {

        super();

        this.name = name;

        this.age = age;

    }


    public static void main(String[] args) {

        try {

            CloneTest cloneTest = new CloneTest("酸辣汤", 18);//todo

            CloneTest copyClone = (CloneTest) cloneTest.clone();

            System.out.println("newclone:" + cloneTest.getName());

            System.out.println("copyClone:" + copyClone.getName());

        } catch (CloneNotSupportedException e) {

            e.printStackTrace();

        }

    }

}


class Person implements Serializable {

    int age;

    int height;


    //newInstance创建对象实例的时候会调用无参的构造函数，所以必需确保类中有无参数的可见的构造函数，否则将会抛出异常。
    //Caused by: java.lang.NoSuchMethodException: Person.<init>()
    public Person() {

    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public int getHeight() {
        return height;
    }

    public void setHeight(int height) {
        this.height = height;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    String name;

    @Override

    public String toString() {
        return "Person{" +

                "name='" + name + '\'' +

                ", age=" + age +

                '}';
    }


    private Person(String name){
        this.name = name;
    }

    private void think(String course) {
        System.out.println(course);
    }


    public Person(String name, int age, int height) {

        this.name = name;

        this.age = age;

        this.height = height;

    }

}


class MyTestSer {

    /**
     * Java对象的序列化与反序列化
     */

    public static void main(String[] args) {

        Person zhangsan = new Person("zhangsan", 30, 170);

        Person lisi = new Person("lisi", 35, 175);

        Person wangwu = new Person("wangwu", 28, 178);

        try {

            //需要一个文件输出流和对象输出流；文件输出流用于将字节输出到文件，对象输出流用于将对象输出为字节

            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream("person.ser"));

            out.writeObject(zhangsan);

            out.writeObject(lisi);

            out.writeObject(wangwu);

        } catch (IOException e) {

            e.printStackTrace();

        }

        try {

            ObjectInputStream in = new ObjectInputStream(new FileInputStream("person.ser"));

            Person one = (Person) in.readObject();

            Person two = (Person) in.readObject();

            Person three = (Person) in.readObject();

            System.out.println("name:" + one.name + " age:" + one.age + " height:" + one.height);

            System.out.println("name:" + two.name + " age:" + two.age + " height:" + two.height);

            System.out.println("name:" + three.name + " age:" + three.age + " height:" + three.height);

        } catch (Exception e) {

            e.printStackTrace();

        }

    }

}


class ClassNewInstance {

    public static void main(String[] args) throws IllegalAccessException, InstantiationException {

        Person person = Person.class.newInstance();

        person.setAge(18);

        person.setName("酸辣汤");
        person.setHeight(190);

        System.out.println(person);

    }

}

class GetConstructor {

    public static void main(String[] args) {

        Class p = Person.class;

        for (Constructor constructor : p.getConstructors()) {

            System.out.println(constructor);

        }

    }

}


class ConstructorInstance {

    public static void main(String[] args) throws Exception {

        Class p = Person.class;

        Constructor constructor1 = p.getConstructor();//获取默认的构造方法

        Constructor constructor2 = p.getConstructor(String.class, int.class, int.class);//获取指定的构造方法

        System.out.println(constructor1);

        System.out.println(constructor2);
        Person person = (Person) constructor2.newInstance("酸辣汤", 18, 190);

        System.out.println(person);

    }

}


class UnsafeCreateObject {

    static Unsafe unsafe;

    static {
        //获取Unsafe对象
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static class C1 {
        private String name;

        private C1() {
            System.out.println("C1 default constructor!");
        }

        private C1(String name) {
            this.name = name;
            System.out.println("C1 有参 constructor!");
        }

        public void test() {

            System.out.println("执行了test方法");
        }
    }

    public static void main(String[] args) throws InstantiationException {
        C1 c = (C1) unsafe.allocateInstance(C1.class);
        System.out.println(c);
        c.test();
    }
}
