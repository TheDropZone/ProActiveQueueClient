package com.ProActiveQueue.ProActiveQueueClient.Connection;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The <b>MessageProperties</b> class allows you to construct filters for topic/queues and is
 * passed in to the MessageClient class or extending classes.
 * <p>
 * The <b>MessageProperties</b> class should be used through the "builder()" method to constructor filters.
 *<p>
 * The MessageProperties class has the following select/filter options:
 * <ul>
 *     <li>
 *         <b>MATCHES</b> "=" (makes sure all messages match on a particular property, regardless of value)
 *     </li>
 *     <li>
 *         <b>EQUALS</b> "=" (requires a value to check equality with)
 *     </li>
 *     <li>
 *         <b>GREATER_THAN</b> ">" (requires a number)
 *     </li>
 *     <li>
 *         <b>LESS_THAN</b> "<" (requires a number)
 *     </li>
 * </ul>
 *
 * <pre>{@code
 * //This filter makes sure that the property 'NmapScanType' matches on all
 * //returned messages, regardless of the actual value of the 'NmapScanType'
 * //property
 * MessageProperties.builder()
 *    .addProperty(NmapScanType.class.getSimpleName(),
 *          MessageProperties.Selector.MATCHES)
 * }</pre>
 *
 * <pre>
 * //This filter makes sure that the property 'NmapScanType' property value equals 'STATUS'
 * //on all returned messages
 * MessageProperties.builder()
 *     .addProperty(NmapScanType.class.getSimpleName(),
 *          MessageProperties.Selector.EQUALS, 'STATUS')
 * </pre>
 */
public class MessageProperties {

    private List<MessageProperty> properties;

    public static MessageProperties builder(){
        return new MessageProperties();
    }

    public MessageProperties(){
        properties = new ArrayList<>();
    }

    /**
     * Standard constructor to initialize the MessageProperties object with a single <b>MATCHES</b> filter
     * @param propertyName The name of the filter property
     * @param selector The filter selector
     */
    public MessageProperties(String propertyName, Selector selector){
        properties = new ArrayList<>();
        properties.add(new MessageProperty(propertyName,selector));
    }

    /**
     * Standard constructor to initialize the MessageProperties object with a single standard filter
     * @param propertyName the name of the filter property
     * @param selector the filter selector
     * @param value the value to filter against
     */
    public MessageProperties(String propertyName, Selector selector, Object value){
        properties = new ArrayList<>();
        properties.add(new MessageProperty(propertyName,selector, value));
    }

    /**
     * Adds a <b>MATCHES</b> filter to the list of message properties
     * @param propertyName the name of the filter property
     * @param selector the filter selector
     * @return Chainable object
     */
    public MessageProperties addProperty(String propertyName, Selector selector){
        properties.add(new MessageProperty(propertyName,selector));
        return this;
    }

    /**
     * Adds a standard filter to the list of message properties
     * @param propertyName the name of the filter property
     * @param selector the filter selector
     * @param value the value to filter against
     * @return Chainable object
     */
    public MessageProperties addProperty(String propertyName, Selector selector, Object value){
        properties.add(new MessageProperty(propertyName,selector, value));
        return this;
    }

    /**
     * Returns all the filters (MessageProperty Objects) that have been registered so far
     * @return A list of MessageProperty Objects
     */
    public List<MessageProperty> getMessageProperties(){
        return this.properties;
    }

    /**
     * Returns all the filters (MessageProperty Objects) that have been registered so far, based on
     * which ones require input or don't require input
     * @param requiresInput whether the filter (MessageProperty) requires input. Eg, MATCHES filter.
     * @return A list of matching MessageProperty Objects
     */
    public List<MessageProperty> getMessageProperties(boolean requiresInput){
        return this.properties.stream()
                .filter(property -> property.getSelector().requiresInput() == requiresInput)
                .collect(Collectors.toList());
    }


    /**
     * This class represents a single filter by which messages can be filtered on. This filter is modeled
     * by the name of the filter property on the message, the type of filter <b>Selector</b> to use, and
     * a filter value to compare against if the selector type requires it.
     */
    public class MessageProperty{
        private String propertyName;
        private Selector selector;
        private Object value;

        /**
         * Standard constructor to initialize the MessageProperties object with a single <b>MATCHES</b> filter
         * @param propertyName The name of the filter property
         * @param selector The filter selector
         */
        public MessageProperty(String propertyName, Selector selector){
            if(selector == Selector.MATCHES){
                this.propertyName = propertyName;
                this.selector = selector;
            }else{
                throw new IllegalArgumentException("This constructor can only be used with MATCHES selector");
            }
        }

        /**
         * Standard constructor to initialize a MessageProperty object
         * @param propertyName the name of the filter property
         * @param selector the filter selector
         * @param value the value to filter against
         */
        public MessageProperty(String propertyName, Selector selector, Object value){
            this.propertyName = propertyName;
            this.selector = selector;
            this.value = value;
            if(selector.requiresNumber && value instanceof String){
                throw new IllegalArgumentException("The " + selector.name() + " selector requires a numeric value");
            }
        }

        /**
         * <p>
         * This utility method is used to turn the information about the message filter into the actual
         * string interpretation used in the ActiveMQ connection.
         * </p>
         * <p>
         * If this filter is a <b>MATCHES</b> type filter, you must use the getInputBasedFilter() method
         * </p>
         * @return A String interpretation of this object (filter)
         */
        public String getFilter(){
            String valueString = "";
            if(value instanceof String){
                valueString = "'" + value + "'";
            }else if(value instanceof Number){
                valueString = value.toString();
            }
            return this.propertyName + this.selector.getSelector() + valueString;
        }

        /**
         * This utility method is used to turn the information about the <b>MATCHES</b> message filter into the actual
         * string interpretation used in the ActiveMQ connection. Because the matches filter type is based on
         * matching a value at runtime while messages are coming in, we don't know what the filter match value will
         * be while programming.
         * @param match the value to filter against. Typically provided during runtime based on incoming messages
         * @return A String interpretation of this object (filter)
         */
        public String getInputBasedFilter(Object match){
            if(selector != Selector.MATCHES){
                throw new IllegalStateException("getMatchingFilter() can only be called on a MATCHES selector");
            }
            String matchString = "";
            if(match instanceof String){
                matchString = "'" + match + "'";
            }else if(match instanceof Number){
                matchString = match.toString();
            }
            return this.propertyName + this.selector.getSelector() + matchString;
        }

        public Selector getSelector(){
            return this.selector;
        }

        public String getPropertyName(){
            return this.propertyName;
        }

        public Object getValue(){
            return this.value;
        }
    }

    /**
     * This Enum models the filter selector that will be used during the filtering of a message.
     * <ul>
     *     <li>
     *         <b>MATCHES</b> "=" (makes sure all messages match on a particular property, regardless of value)
     *     </li>
     *     <li>
     *         <b>EQUALS</b> "=" (requires a value to check equality with)
     *     </li>
     *     <li>
     *         <b>GREATER_THAN</b> ">" (requires a number)
     *     </li>
     *     <li>
     *         <b>LESS_THAN</b> "<" (requires a number)
     *     </li>
     * </ul>
     */
    public enum Selector{
        MATCHES("=", false, true),
        EQUALS("=", false, false),
        GREATER_THAN(">", true, false),
        LESS_THAN("<", true, false);

        /** The ActiveMQ selector string that will be used when converting this filter to a string interpretation */
        private String selector;

        /**Whether this filter requires a number. Greater than and less than are great examples */
        private boolean requiresNumber;

        /**Whether this selector requires input at runtime. The <b>MATCHES</b> filter requires input at runtime */
        private boolean requiresInput;

        private Selector(String selector, boolean requiresNumber, boolean requiresInput){
            this.selector = selector;
            this.requiresNumber = requiresNumber;
            this.requiresInput = requiresInput;
        }

        public String getSelector(){
            return this.selector;
        }

        public boolean requiresNumber(){
            return this.requiresNumber;
        }

        public boolean requiresInput(){
            return requiresInput;
        }
    }
}
