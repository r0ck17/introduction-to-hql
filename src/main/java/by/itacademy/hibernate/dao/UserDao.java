package by.itacademy.hibernate.dao;


import by.itacademy.hibernate.entity.Payment;
import by.itacademy.hibernate.entity.User;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.hibernate.Session;

import java.util.List;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class UserDao {

    private static final UserDao INSTANCE = new UserDao();

    /**
     * Возвращает всех сотрудников
     */
    public List<User> findAll(Session session) {
        return session.createQuery("FROM User", User.class)
                .list();
    }

    /**
     * Возвращает всех сотрудников с указанным именем
     */
    public List<User> findAllByFirstName(Session session, String firstName) {
        String pattern = firstName + "%";
        return session.createQuery("""
                        FROM User u WHERE u.username LIKE :pattern
                        """, User.class)
                .setParameter("pattern", pattern)
                .list();
    }

    /**
     * Возвращает первые {limit} сотрудников, упорядоченных по дате рождения (в порядке возрастания)
     */
    public List<User> findLimitedUsersOrderedByBirthday(Session session, int limit) {
        // TODO : implement
        return session.createQuery("""
                        SELECT u
                        FROM User u
                        ORDER BY u.personalInfo.birthDate ASC
                        """, User.class)
                .setMaxResults(limit)
                .getResultList();
    }

    /**
     * Возвращает всех сотрудников компании с указанным названием
     */
    public List<User> findAllByCompanyName(Session session, String companyName) {
        return session.createQuery("""
                        FROM User u 
                        WHERE u.company.name = :company
                        """, User.class)
                .setParameter("company", companyName)
                .list();
    }

    /**
     * Возвращает все выплаты, полученные сотрудниками компании с указанными именем,
     * упорядоченные по имени сотрудника, а затем по размеру выплаты
     */
    public List<Payment> findAllPaymentsByCompanyName(Session session, String companyName) {
        return session.createQuery("""
                        FROM Payment p
                        WHERE p.receiver.company.name = :company
                        """, Payment.class)
                .setParameter("company", companyName)
                .list();
    }

    /**
     * Возвращает среднюю зарплату сотрудника с указанными именем и фамилией
     */
    public Double findAveragePaymentAmountByFirstAndLastNames(Session session, String firstName, String lastName) {
        return session.createQuery("""
                SELECT AVG(p.amount) 
                FROM Payment p
                WHERE p.receiver.personalInfo.firstname = :firstName AND p.receiver.personalInfo.lastname = :lastName
                """, Double.class)
                .setParameter("firstName", firstName)
                .setParameter("lastName", lastName)
                .getSingleResult();
    }

    /**
     * Возвращает для каждой компании: название, среднюю зарплату всех её сотрудников. Компании упорядочены по названию.
     */
    public List<Object[]> findCompanyNamesWithAvgUserPaymentsOrderedByCompanyName(Session session) {
        return session.createQuery("""
                SELECT c.name, AVG(p.amount) 
                FROM Company c JOIN c.users u JOIN u.payments p
                GROUP BY c.name
                ORDER BY c.name
                """, Object[].class)
                .list();
    }

    /**
     * Возвращает список: сотрудник (объект User), средний размер выплат, но только для тех сотрудников, чей средний размер выплат
     * больше среднего размера выплат всех сотрудников
     * Упорядочить по имени сотрудника
     */
    public List<Object[]> isItPossible(Session session) {
        return session.createQuery("""
                SELECT u, AVG(p.amount)
                FROM User u JOIN u.payments p
                GROUP BY u
                HAVING avg(p.amount) > (
                    SELECT AVG(p2.amount)
                    FROM Payment p2
                )
                """, Object[].class)
                .list();
    }

    public static UserDao getInstance() {
        return INSTANCE;
    }
}