package org.apache.oozie.tools.workflowgenerator.client.property;

/**
 * class to provide key-value pair for property, mostly used in property table
 */
public class Property {

    private String name;
    private String value;

    /**
     * Constructor which records name and value
     *
     * @param name name
     * @param value value
     */
    public Property(String name, String value) {
        this.name = name;
        this.value = value;
    }

    /**
     * Return a name of property
     *
     * @return
     */
    public String getName() {
        return name;
    }

    /**
     * Set a name of property
     *
     * @param name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Return a value of property
     *
     * @return
     */
    public String getValue() {
        return value;
    }

    /**
     * Set a value of property
     *
     * @param value
     */
    public void setValue(String value) {
        this.value = value;
    }
}