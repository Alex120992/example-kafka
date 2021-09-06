package lessons4.customschema_avro;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class SomeObject {
    private String name;
    private String surname;
    private int age;
}
