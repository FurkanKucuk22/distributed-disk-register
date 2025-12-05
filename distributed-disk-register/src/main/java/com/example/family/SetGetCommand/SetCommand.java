package com.example.family.SetGetCommand;

public class SetCommand implements Command {

    private final String key;
    private final String value;

    public SetCommand(String key, String value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public String execute(DataStore store) {
        return store.set(key, value);
    }
}


