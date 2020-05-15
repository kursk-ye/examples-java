package io.github.streamingwithflink.chapter8;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;

public class CommonUtils {

    public static byte[] convertObjToBytesArray(Object o) throws IOException {
        ByteArrayOutputStream bos;
        ObjectOutputStream oos;

        bos = new ByteArrayOutputStream();
        oos = new ObjectOutputStream(bos);
        oos.writeObject(o);
        oos.flush();

        return bos.toByteArray();

    }

    /**
     *
     * @param o 要执行方法的对象
     * @param fieldName 属性名，被执行的方法是 get+属性名，属性名首字母大写
     * @param type 对象o的类型
     * @param <T> 对象o的类
     * @return
     * @throws Throwable
     */
    public static <T> Object invokeGetterMethod(Object o,String fieldName,Class<T> type) throws Throwable {
        if (type.getDeclaredField(fieldName) == null) {
            throw new Throwable("this filed name not exit");
        } else {
            String getterName = "get" + capitalizeFirstLetter(fieldName); // getter method name first UPPER!!!!!!!
            Method fieldHandler = type.getDeclaredMethod(getterName);
            Object fieldValue = fieldHandler.invoke(o);
            return  fieldValue;
        }
    }

    private static String capitalizeFirstLetter(String str) {
        return str.toLowerCase().substring(0, 1).toUpperCase() + str.toLowerCase().substring(1);
    }
}
