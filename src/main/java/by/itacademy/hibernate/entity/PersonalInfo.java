package by.itacademy.hibernate.entity;

import by.itacademy.hibernate.convertor.BirthdayConvertor;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Column;
import javax.persistence.Convert;
import javax.persistence.Embeddable;
import java.io.Serial;
import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Embeddable
public class PersonalInfo implements Serializable {

    @Serial
    private static final long serialVersionUID = -3403693165403941753L;

    private String firstname;
    private String lastname;
    @Convert(converter = BirthdayConvertor.class)
    @Column(name = "birth_date")
    private Birthday birthDate;
}
