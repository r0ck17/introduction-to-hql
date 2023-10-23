package by.itacademy.hibernate.dao;


import by.itacademy.hibernate.entity.Chat;
import by.itacademy.hibernate.entity.Company;
import by.itacademy.hibernate.utils.TestDataImporter;
import by.itacademy.hibernate.entity.Payment;
import by.itacademy.hibernate.entity.User;
import by.itacademy.hibernate.util.HibernateUtil;
import com.querydsl.core.Tuple;
import lombok.Cleanup;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Optional;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
class DaoTest {

    private final SessionFactory sessionFactory = HibernateUtil.buildSessionFactory();
    private final Dao dao = Dao.getInstance();

    @BeforeAll
    public void initDb() {
        TestDataImporter.importData(sessionFactory);
    }

    @AfterAll
    public void finish() {
        sessionFactory.close();
    }

    @Test
    void findAll() {
        @Cleanup Session session = sessionFactory.openSession();
        session.beginTransaction();

        List<User> results = dao.findAll(session);
        assertThat(results).hasSize(5);

        List<String> fullNames = results.stream().map(User::fullName).collect(toList());
        assertThat(fullNames).containsExactlyInAnyOrder("Bill Gates", "Steve Jobs", "Sergey Brin", "Tim Cook", "Diane Greene");

        session.getTransaction().commit();
    }

    @Test
    void findAllByFirstName() {
        @Cleanup Session session = sessionFactory.openSession();
        session.beginTransaction();

        List<User> results = dao.findAllByFirstName(session, "Bill");

        assertThat(results).hasSize(1);
        assertThat(results.get(0).fullName()).isEqualTo("Bill Gates");

        session.getTransaction().commit();
    }

    @Test
    void findLimitedUsersOrderedByBirthday() {
        @Cleanup Session session = sessionFactory.openSession();
        session.beginTransaction();

        int limit = 3;
        List<User> results = dao.findLimitedUsersOrderedByBirthday(session, limit);
        assertThat(results).hasSize(limit);

        List<String> fullNames = results.stream().map(User::fullName).collect(toList());
        assertThat(fullNames).contains("Diane Greene", "Steve Jobs", "Bill Gates");

        session.getTransaction().commit();
    }

    @Test
    void findAllByCompanyName() {
        @Cleanup Session session = sessionFactory.openSession();
        session.beginTransaction();

        List<User> results = dao.findAllByCompanyName(session, "Google");
        assertThat(results).hasSize(2);

        List<String> fullNames = results.stream().map(User::fullName).collect(toList());
        assertThat(fullNames).containsExactlyInAnyOrder("Sergey Brin", "Diane Greene");

        session.getTransaction().commit();
    }

    @Test
    void findAllPaymentsByCompanyName() {
        @Cleanup Session session = sessionFactory.openSession();
        session.beginTransaction();

        List<Payment> applePayments = dao.findAllPaymentsByCompanyName(session, "Apple");
        assertThat(applePayments).hasSize(5);

        List<Integer> amounts = applePayments.stream().map(Payment::getAmount).collect(toList());
        assertThat(amounts).contains(250, 500, 600, 300, 400);

        session.getTransaction().commit();
    }

    @Test
    void findAveragePaymentAmountByFirstAndLastNames() {
        @Cleanup Session session = sessionFactory.openSession();
        session.beginTransaction();

        Double averagePaymentAmount = dao.findAveragePaymentAmountByFirstAndLastNames(session, "Bill", "Gates");
        assertThat(averagePaymentAmount).isEqualTo(300.0);

        session.getTransaction().commit();
    }

    @Test
    void findCompanyNamesWithAvgUserPaymentsOrderedByCompanyName() {
        @Cleanup Session session = sessionFactory.openSession();
        session.beginTransaction();

        List<Tuple> results = dao.findCompanyNamesWithAvgUserPaymentsOrderedByCompanyName(session);
        assertThat(results).hasSize(3);

        List<String> orgNames = results.stream().map(a -> a.get(0, String.class)).collect(toList());
        assertThat(orgNames).contains("Apple", "Google", "Microsoft");

        List<Double> orgAvgPayments = results.stream().map(a -> a.get(1, Double.class)).collect(toList());
        assertThat(orgAvgPayments).contains(410.0, 400.0, 300.0);

        session.getTransaction().commit();
    }

    @Test
    void isItPossible() {
        @Cleanup Session session = sessionFactory.openSession();
        session.beginTransaction();

        List<Tuple> results = dao.isItPossible(session);
        assertThat(results).hasSize(2);

        List<String> names = results.stream().map(r -> r.get(0, User.class).fullName()).collect(toList());
        assertThat(names).contains("Sergey Brin", "Steve Jobs");

        List<Double> averagePayments = results.stream().map(r -> r.get(1, Double.class)).collect(toList());
        assertThat(averagePayments).contains(500.0, 450.0);

        session.getTransaction().commit();
    }

    @Test
    void findUsersCountInChats() {
        @Cleanup Session session = sessionFactory.openSession();
        session.beginTransaction();

        List<Tuple> results = dao.findUsersCountInChats(session);
        assertThat(results).hasSize(3);

        List<String> names = results.stream().map(r -> r.get(0, String.class)).collect(toList());
        assertThat(names).contains("chat 1", "chat 2", "chat 3");

        List<Long> averagePayments = results.stream().map(r -> r.get(1, Long.class)).collect(toList());
        assertThat(averagePayments).contains(2L, 1L, 2L);

        session.getTransaction().commit();
    }

    @Test
    void findUsersByLanguage() {
        @Cleanup Session session = sessionFactory.openSession();
        session.beginTransaction();

        List<User> users = dao.findUsersByLanguage(session, "en");
        assertThat(users).hasSize(2);

        List<String> names = users.stream().map(User::getUsername).collect(toList());
        assertThat(names).contains("SergeyBrin", "TimCook");

        session.getTransaction().commit();
    }

    @Test
    void findBiggestPayment() {
        @Cleanup Session session = sessionFactory.openSession();
        session.beginTransaction();

        Optional<Payment> biggestPayment = dao.findBiggestPayment(session);
        assertTrue(biggestPayment.isPresent());
        assertEquals(600, biggestPayment.get().getAmount());

        session.getTransaction().commit();
    }

    @Test
    void findUsersWithName() {
        @Cleanup Session session = sessionFactory.openSession();
        session.beginTransaction();

        List<User> users = dao.findUsersWithName(session, "Bill");
        assertThat(users).hasSize(1);

        session.getTransaction().commit();
    }

    @Test
    void findUsersCompaniesInChat() {
        @Cleanup Session session = sessionFactory.openSession();
        session.beginTransaction();

        Chat chat = session.find(Chat.class, 1L);

        List<Company> companies = dao.findUsersCompaniesInChat(session, chat);
        assertThat(companies).hasSize(2);

        List<String> names = companies.stream().map(Company::getName).toList();

        assertThat(names).contains("Microsoft", "Apple");

        session.getTransaction().commit();
    }
}
