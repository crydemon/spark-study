package think;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class ThinkProxy {
}

interface PersonInterface {
    void doSomething();

    void saySomething();
}

class PersonImpl implements PersonInterface {
    @Override
    public void doSomething() {
        System.out.println("人类在做事");
    }

    @Override
    public void saySomething() {
        System.out.println("人类在说话");
    }
}

class PersonProxy {
    public static void main(String[] args) {
        final PersonImpl person = new PersonImpl();
        PersonInterface proxyPerson = (PersonInterface) Proxy.newProxyInstance(PersonImpl.class.getClassLoader(),
                PersonImpl.class.getInterfaces(), new InvocationHandler() {
                    //在下面的invoke方法里面写我们的业务
                    @Override
                    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                        if (method.getName() == "doSomething") {
                            person.doSomething();
                            System.out.println("通过常规方法调用了实现类");
                        } else {
                            method.invoke(person, args);
                            System.out.println("通过反射机制调用了实现类");
                        }
                        return null;
                    }
                });
        proxyPerson.doSomething();
        proxyPerson.saySomething();
    }
}
