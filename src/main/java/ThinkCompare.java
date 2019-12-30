import java.util.*;

class ThinkCompare {
    private static class Person implements Comparable<Person> {

        String name;
        int age;
        public Person(String name, int age){
            this.name = name;
            this.age = age;
        }
        @Override
        public int compareTo(Person person) {
            return name.compareTo(person.name);
            //return this.name - person.name;
        }
        @Override
        public String toString() {
            return String.format("{name=%s, age=%d}", name, age);
        }
    }
    public static void main(String[] args) {
        ArrayList<Person> list = new ArrayList<Person>();
        list.add(new Person("aaa", 10));
        list.add(new Person("bbb", 20));
        list.add(new Person("ccc", 30));
        list.add(new Person("ddd", 40));
        Collections.sort(list);
        System.out.println(list);

    }
}




class ComparatorDemo {


    static class Person {
        String name;
        int age;
        Person(String n, int a) {
            name = n;
            age = a;
        }
        @Override
        public String toString() {
            return String.format("{name=%s, age=%d}", name, age);
        }
    }
    public static void main(String[] args) {
        List<Person> people = Arrays.asList(
                new Person("Joe", 24),
                new Person("Pete", 18),
                new Person("Chris", 21)
        );
        //Collections.sort(people, (Person o1, Person o2) -> o1.name.compareTo(o2.name));
        Collections.sort(people, Comparator.comparing((Person o) -> o.name));
        System.out.println(people);

    }
}


