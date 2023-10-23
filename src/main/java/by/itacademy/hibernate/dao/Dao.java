package by.itacademy.hibernate.dao;

import by.itacademy.hibernate.entity.Chat;
import by.itacademy.hibernate.entity.Company;
import by.itacademy.hibernate.entity.Payment;
import by.itacademy.hibernate.entity.User;
import com.querydsl.core.Tuple;
import com.querydsl.core.types.Order;
import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.jpa.impl.JPAQuery;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.hibernate.Session;

import java.util.List;
import java.util.Optional;

import static by.itacademy.hibernate.entity.QChat.chat;
import static by.itacademy.hibernate.entity.QCompany.company;
import static by.itacademy.hibernate.entity.QPayment.payment;
import static by.itacademy.hibernate.entity.QProfile.profile;
import static by.itacademy.hibernate.entity.QUser.user;
import static by.itacademy.hibernate.entity.QUserChat.userChat;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Dao {

    private static final Dao INSTANCE = new Dao();

    /**
     * Возвращает всех сотрудников
     */
    public List<User> findAll(Session session) {
        return new JPAQuery<User>(session)
                .select(user)
                .from(user)
                .fetch();
    }

    /**
     * Возвращает всех сотрудников с указанным именем
     */
    public List<User> findAllByFirstName(Session session, String firstName) {
        return new JPAQuery<User>(session)
                .select(user)
                .from(user)
                .where(user.personalInfo.firstname.eq(firstName))
                .fetch();
    }

    /**
     * Возвращает первые {limit} сотрудников, упорядоченных по дате рождения (в порядке возрастания)
     */
    public List<User> findLimitedUsersOrderedByBirthday(Session session, int limit) {
        return new JPAQuery<User>(session)
                .select(user)
                .from(user)
                .orderBy(new OrderSpecifier(Order.ASC, user.personalInfo.birthDate))
                .limit(limit)
                .fetch();
    }

    /**
     * Возвращает всех сотрудников компании с указанным названием
     */
    public List<User> findAllByCompanyName(Session session, String companyName) {
        return new JPAQuery<User>(session)
                .select(user)
                .from(company)
                .join(company.users, user)
                .where(company.name.eq(companyName))
                .fetch();
    }

    /**
     * Возвращает все выплаты, полученные сотрудниками компании с указанными именем,
     * упорядоченные по имени сотрудника, а затем по размеру выплаты
     */
    public List<Payment> findAllPaymentsByCompanyName(Session session, String companyName) {
        return new JPAQuery<Payment>(session)
                .select(payment)
                .from(company)
                .join(company.users, user)
                .join(user.payments, payment)
                .where(company.name.eq(companyName))
                .orderBy(user.personalInfo.firstname.asc(), payment.amount.asc())
                .fetch();
    }

    /**
     * Возвращает среднюю зарплату сотрудника с указанными именем и фамилией
     */
    public Double findAveragePaymentAmountByFirstAndLastNames(Session session, String firstName, String lastName) {
        return new JPAQuery<Double>(session)
                .select(payment.amount.avg())
                .from(payment)
                .join(payment.receiver, user)
                .where(user.personalInfo.firstname.eq(firstName)
                        .and(user.personalInfo.lastname.eq(lastName)))
                .fetchOne();

    }

    /**
     * Возвращает для каждой компании: название, среднюю зарплату всех её сотрудников. Компании упорядочены по названию.
     */
    public List<Tuple> findCompanyNamesWithAvgUserPaymentsOrderedByCompanyName(Session session) {
        return new JPAQuery<Tuple>(session)
                .select(company.name, payment.amount.avg())
                .from(company)
                .join(company.users, user)
                .join(user.payments, payment)
                .groupBy(company.name)
                .orderBy(company.name.asc())
                .fetch();
    }

    /**
     * Возвращает список: сотрудник (объект User), средний размер выплат, но только для тех сотрудников, чей средний размер выплат
     * больше среднего размера выплат всех сотрудников
     * Упорядочить по имени сотрудника
     */
    public List<Tuple> isItPossible(Session session) {
        return new JPAQuery<Tuple>(session)
                .select(user, payment.amount.avg())
                .from(user)
                .join(user.payments, payment)
                .groupBy(user.id)
                .having(payment.amount.avg().gt(
                        new JPAQuery<Double>(session)
                                .select(payment.amount.avg())
                                .from(payment)
                ))
                .orderBy(user.personalInfo.firstname.asc())
                .fetch();
    }

    /**
     * Возвращает название чата и количество пользователей, находящихся в нем
     */
    public List<Tuple> findUsersCountInChats(Session session) {
        return new JPAQuery<User>(session)
                .select(chat.name, userChat.count())
                .from(userChat)
                .join(userChat.chat, chat)
                .groupBy(chat.name)
                .fetch();
    }

    /**
     * Возвращает список пользователей с указанным языком в профиле, отсортированных в алфавитном порядке по username
     */
    public List<User> findUsersByLanguage(Session session, String language) {
        return new JPAQuery<User>(session)
                .select(user)
                .from(user)
                .join(user.profile, profile)
                .where(user.profile.language.eq(language))
                .orderBy(user.username.asc())
                .fetch();
    }

    /**
     * Возвращает самый большой платеж
     */
    public Optional<Payment> findBiggestPayment(Session session) {
        return Optional.ofNullable(
                new JPAQuery<Payment>(session)
                        .select(payment)
                        .from(payment)
                        .orderBy(payment.amount.desc())
                        .fetchFirst());
    }


    /**
     * Возвращает список всех пользователей с указанным именем
     */
    public List<User> findUsersWithName(Session session, String name) {
        return new JPAQuery<User>(session)
                .select(user)
                .from(user)
                .where(user.personalInfo.firstname.eq(name))
                .fetch();
    }

    /**
     * Возвращает список компаний, сотрудники которых сидят в определенном чате
     */
    public List<Company> findUsersCompaniesInChat(Session session, Chat chat) {
        return new JPAQuery<Company>(session)
                .select(user.company)
                .from(userChat)
                .join(userChat.user, user)
                .where(userChat.chat.eq(chat))
                .fetch();

    }
    public static Dao getInstance() {
        return INSTANCE;
    }
}