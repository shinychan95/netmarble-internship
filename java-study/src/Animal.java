public class Animal {
    String name;

    public void setName(String name){
        this.name = name;
    }

    public static void main(String args[]){
        Animal cat = new Animal();
        cat.setName("BakCass");
        System.out.println(cat.name);
    }
}
