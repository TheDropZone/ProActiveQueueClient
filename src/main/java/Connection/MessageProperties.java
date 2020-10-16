package Connection;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The MessageProperties class allows you to construct filters for topic/queues and is
 * passed in to the MessageClient class or extending classes.
 *
 * The MessageProperties class should be used through the "builder()" method to constructor filters.
 *
 * The MessageProperties class has the following select/filter options:
 * MATCHES "=" (makes sure all messages match on a particular property, regardless of value)
 * EQUALS "=" (requires a value to check equality with)
 * GREATER_THAN ">" (requires a number)
 * LESS_THAN "<" (requires a number)
 *
 *
 * <code>
 *     //This filter makes sure that the property 'NmapScanType' matches on all returned messages,
 *     //regardless of the actual value of the 'NmapScanType' property
 *     MessageProperties.builder()
 *          .addProperty(NmapScanType.class.getSimpleName(), MessageProperties.Selector.MATCHES)
 * </code>
 *
 * <code>
 *     //This filter makes sure that the property 'NmapScanType' property value equals 'STATUS'
 *     //on all returned messages
 *     MessageProperties.builder()
 *          .addProperty(NmapScanType.class.getSimpleName(), MessageProperties.Selector.EQUALS, 'STATUS')
 * </code>
 */
public class MessageProperties {

    private List<MessageProperty> properties;

    public static MessageProperties builder(){
        return new MessageProperties();
    }

    public MessageProperties(){
        properties = new ArrayList<>();
    }

    public MessageProperties(String propertyName, Selector selector){
        properties = new ArrayList<>();
        properties.add(new MessageProperty(propertyName,selector));
    }

    public MessageProperties(String propertyName, Selector selector, Object value){
        properties = new ArrayList<>();
        properties.add(new MessageProperty(propertyName,selector, value));
    }

    public MessageProperties addProperty(String propertyName, Selector selector){
        properties.add(new MessageProperty(propertyName,selector));
        return this;
    }

    public MessageProperties addProperty(String propertyName, Selector selector, Object value){
        properties.add(new MessageProperty(propertyName,selector, value));
        return this;
    }

    public List<MessageProperty> getMessageProperties(){
        return this.properties;
    }

    public List<MessageProperty> getMessageProperties(boolean requiresInput){
        return this.properties.stream()
                .filter(property -> property.getSelector().requiresInput() == requiresInput)
                .collect(Collectors.toList());
    }


    public class MessageProperty{
        private String propertyName;
        private Selector selector;
        private Object value;

        public MessageProperty(String propertyName, Selector selector){
            if(selector == Selector.MATCHES){
                this.propertyName = propertyName;
                this.selector = selector;
            }else{
                throw new IllegalArgumentException("This constructor can only be used with MATCHES selector");
            }
        }

        public MessageProperty(String propertyName, Selector selector, Object value){
            this.propertyName = propertyName;
            this.selector = selector;
            this.value = value;
            if(selector.requiresNumber && value instanceof String){
                throw new IllegalArgumentException("The " + selector.name() + " selector requires a numeric value");
            }
        }

        public String getFilter(){
            String valueString = "";
            if(value instanceof String){
                valueString = "'" + value + "'";
            }else if(value instanceof Number){
                valueString = value.toString();
            }
            return this.propertyName + this.selector.getSelector() + valueString;
        }

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

    public enum Selector{
        MATCHES("=", false, true),
        EQUALS("=", false, false),
        GREATER_THAN(">", true, false),
        LESS_THAN("<", true, false);

        private String selector;
        private boolean requiresNumber;
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
