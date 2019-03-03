package top.ljming.javaSparkLearn.model;

import java.util.Objects;

public class Person {

    private String name;
    private String address;
    private String company;
    private Integer age;
    private String gender;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getCompany() {
        return company;
    }

    public void setCompany(String company) {
        this.company = company;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Person person = (Person) o;
        return Objects.equals(name, person.name) &&
                Objects.equals(address, person.address) &&
                Objects.equals(company, person.company) &&
                Objects.equals(age, person.age) &&
                Objects.equals(gender, person.gender);
    }

    @Override
    public int hashCode() {

        return Objects.hash(name, address, company, age, gender);
    }

    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                ", address='" + address + '\'' +
                ", company='" + company + '\'' +
                ", age=" + age +
                ", gender='" + gender + '\'' +
                '}';
    }

}
