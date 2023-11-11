package org.example;

import java.util.Scanner;


class Main {

    public static void main(String[] args) {
        citesteArgumente(args);

        Scanner scanner = new Scanner(System.in);
        Interpretor interpretor = new Interpretor();

        String sir = "";
        do {
            System.out.print(">");
            sir = scanner.nextLine();
            interpretor.interpreteaza(sir);
        } while (!sir.equals("bye"));

        Producer.getInstance().close();
        Consumer.getInstance().close();
    }

    private static void citesteArgumente(String[] args) {
        if (args.length == 0) {
            System.out.println("No command line arguments provided.");
            System.exit(-1);
        } else {
            /*
            System.out.println("Command line arguments:");
            for (int i = 0; i < args.length; i++) {
                System.out.printf("Argument %d: %s%n", i, args[i]);
            }
             */
            Config.BOOTSTRAP_SERVER_IP = args[0];
            Config.BOOTSTRAP_SERVER_PORT = args[1];
        }
    }
}
