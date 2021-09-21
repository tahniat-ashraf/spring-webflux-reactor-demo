package com.priyam.webflux.reactor.demo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigInteger;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@SpringBootTest
public class ReactorTest {

    private Student messi = Student.builder().age(21).fName("Lionel").lName("Messi").id(1).registered(true).lastPaidInYear("2021").build();
    private Student ronaldo = Student.builder().age(23).fName("Cristiano").lName("Ronaldo").registered(false).id(2).build();
    private Student buffon = Student.builder().age(31).fName("Gigi").lName("Buffon").id(3).registered(false).build();
    private Student zlatan = Student.builder().age(29).fName("Zlatan").lName("Ibrahimovic").id(4).registered(true).lastPaidInYear("2020").build();
    private Student mbappe = Student.builder().age(19).fName("Kylian").lName("Mbappe").id(5).registered(true).lastPaidInYear("2019").build();
    private Student pedri = Student.builder().age(16).fName("Pedri").lName("Gonzalez").id(6).registered(true).lastPaidInYear("2021").build();

    private Flux<Student> getStudents() {

        return Flux.fromIterable(List.of(
                messi,
                ronaldo,
                buffon,
                zlatan,
                mbappe,
                pedri
        ));
    }

    //confirm that every one's age is >=16
    @Test
    public void test1() {

        Mono<Boolean> all = getStudents()
                .all(student -> student.getAge() >= 16);

        StepVerifier.create(all)
                .expectNext(true)
                .verifyComplete();
    }

    //confirm that no one's age is >=35
    @Test
    public void test2() {

        Mono<Boolean> any = getStudents()
                .any(student -> student.getAge() >= 16);

        StepVerifier.create(any)
                .expectNext(true)
                .verifyComplete();
    }

    //everyone who's id is odd number
    @Test
    public void test3() {

        Flux<Student> filter = getStudents()
                .filter(student -> student.getId() % 2 != 0);

        StepVerifier.create(filter)
                .expectNext(messi, buffon, mbappe)
                .verifyComplete();
    }

    //everyone who's id is odd number, create a map of firstName & their id
    @Test
    public void test4() {

        Mono<Map<String, Integer>> mapMono = getStudents()
                .filter(student -> student.getId() % 2 != 0)
                .collectMap(Student::getFName, Student::getId);

        StepVerifier.create(mapMono)
                .expectNext(Map.of(messi.getFName(), messi.getId(),
                        buffon.getFName(), buffon.getId(),
                        mbappe.getFName(), mbappe.getId()))
                .verifyComplete();
    }


    //return students who are registered and paid their due in the year 2021
    @Test
    public void test5() {

        Mono<List<Student>> collect = getStudents()
                .flatMap(student -> isRegistered(student)
                        .filter(isRegistered -> isRegistered)//only the registered students pass this
                        .flatMap(isRegistered -> isDuePaidIn2021(student))//messi, zlatan, kylian, pedri passes
                        .filter(isDuePaid -> isDuePaid)//only messi, pedri passes
                        .map(isValid -> student)
                )
                .collect(Collectors.toList());

        StepVerifier.create(collect)
                .expectNext(List.of(messi, pedri))
                .verifyComplete();

    }

    //return students who are either unregistered or haven't paid their due in the year 2021
    @Test
    public void test6() {
        Mono<List<Student>> collect = getStudents()
                .flatMap(student -> isRegistered(student)
                        .filter(isRegistered -> !isRegistered)
                        .switchIfEmpty(Mono.defer(() -> isDuePaidIn2021(student))//if not deferred, isDuePaidIn2021 will get evaluated immediately and will cause NPE for ronaldo since he doesn't have lastPaidInYear
                                .filter(isDuePaid -> !isDuePaid)
                        )
                        .map(aBoolean -> student)
                ).collect(Collectors.toList());

        StepVerifier.create(collect)
                .expectNext(List.of(ronaldo, buffon, zlatan, mbappe))
                .verifyComplete();


    }

    //sort them by their age+id : if age is 31, id 1 then combined number is 311. sort by high -> low
    @Test
    public void test7() {
        Mono<List<Student>> listMono = getStudents()
                .sort(Comparator.comparing(this::getMagicNumber).reversed())//if reversed is not mentioned, it will sort by low -> high
                .collectList();


        StepVerifier.create(listMono)
                .expectNext(List.of(buffon, zlatan, ronaldo, messi, mbappe, pedri))
                .verifyComplete();
    }

    private BigInteger getMagicNumber(Student student) {
        return new BigInteger(student.getAge() + "" + student.getId());
    }

    private Mono<Boolean> isDuePaidIn2021(Student student) {
        return Mono.just(student.getLastPaidInYear().equalsIgnoreCase("2021"));
    }


    private Mono<Boolean> isRegistered(Student student) {
        return Mono.just(student.isRegistered());
    }


}

@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
class Student {
    private int age;
    private String fName;//first name
    private String lName;//last name
    private int id;
    private boolean registered;//whether registered or not
    private String lastPaidInYear;//fee last paid in which year
}
