package lesson_custom_deser;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SomeObject {
    private String name;
    private String surname;
    private int age;
}
