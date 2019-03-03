package top.ljming.javaSparkLearn.common;

import com.alibaba.fastjson.JSONObject;
import top.ljming.javaSparkLearn.model.Person;

public class LearnUtils {

    public static Person buildPerson(JSONObject jObj) {
        Person person = new Person();
        try {

            String name = (String) jObj.get("name");
            String address = (String) jObj.get("address");
            String gender = (String) jObj.get("gender");
            String company = (String) jObj.get("company");
            Integer age = (Integer) jObj.get("age");

            person.setName(name);
            person.setAddress(address);
            person.setGender(gender);
            person.setCompany(company);
            person.setAge(age);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return person;
    }

    public static Person buildPerson(String line) {
        Person p = new Person();
        String[] lines = line.split("\t");
        p.setName(lines[0]);
        p.setAddress(lines[1]);
        p.setGender(lines[2]);
        p.setCompany(lines[3]);
        p.setAge(Integer.valueOf(lines[4]));
        return p;
    }
}
