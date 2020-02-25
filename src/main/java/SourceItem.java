public class SourceItem {
    private String someStringField1;
    private String someStringField2;
    private int someIntField;

    private String payload; // json string containing thing we want to filter on

    public String getSomeStringField1() {
        return someStringField1;
    }

    public void setSomeStringField1(String someStringField1) {
        this.someStringField1 = someStringField1;
    }

    public String getSomeStringField2() {
        return someStringField2;
    }

    public void setSomeStringField2(String someStringField2) {
        this.someStringField2 = someStringField2;
    }

    public int getSomeIntField() {
        return someIntField;
    }

    public void setSomeIntField(int someIntField) {
        this.someIntField = someIntField;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }
}
