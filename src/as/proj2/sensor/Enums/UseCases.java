package as.proj2.sensor.Enums;

public enum UseCases {
    Uc1,
    Uc2,
    Uc3,
    Uc4,
    Uc5,
    Uc6;

    public int getKafkaProducersCount() {
        if (this == Uc1 || this == Uc6 || this == Uc5) {
            return 1;
        } else if (this == Uc2 || this == Uc4)
            return 6;
        else if (this == Uc3) {
            return 3;
        }

        return 1;
    }

    public int getKafkaConsumersCount() {
        if (this == Uc1) {
            return 1;
        } else if (this == Uc2)
            return 6;
        else if (this == Uc3 || this == Uc6) {
            return 3;
        } else if (this == Uc5) {
            return 9;
        }

        return 1;
    }

    public int getKafkaConsumerGroupsCount() {
        if (this == Uc5) {
            return 3;
        }

        return 1;
    }
}
