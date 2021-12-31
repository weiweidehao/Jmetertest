import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Collectors;


public class PlaceholderResolver {
    /**
     * 默认前缀占位符
     */
    public static final String DEFAULT_PLACEHOLDER_PREFIX = "${";

    /**
     * 默认后缀占位符
     */
    public static final String DEFAULT_PLACEHOLDER_SUFFIX = "}";

    /**
     * 默认单例解析器
     */
    private static final PlaceholderResolver defaultResolver = new PlaceholderResolver();

    /**
     * 占位符前缀
     */
    private String placeholderPrefix = DEFAULT_PLACEHOLDER_PREFIX;

    /**
     * 占位符后缀
     */
    private String placeholderSuffix = DEFAULT_PLACEHOLDER_SUFFIX;

    static Random rand = new Random();

    public PlaceholderResolver(){}

    private PlaceholderResolver(String placeholderPrefix, String placeholderSuffix) {
        this.placeholderPrefix = placeholderPrefix;
        this.placeholderSuffix = placeholderSuffix;
    }

    /**
     * 获取默认的占位符解析器，即占位符前缀为"${", 后缀为"}"
     * @return
     */
    public static PlaceholderResolver getDefaultResolver() {
        return defaultResolver;
    }

    public static PlaceholderResolver getResolver(String placeholderPrefix, String placeholderSuffix) {
        return new PlaceholderResolver(placeholderPrefix, placeholderSuffix);
    }
    public String resolveByRule(String content, Function<String, String> rule) {
        int start = content.indexOf(this.placeholderPrefix);
        if (start == -1) {
            return content;
        }
        StringBuilder result = new StringBuilder(content);
        while (start != -1) {
            int end = result.indexOf(this.placeholderSuffix, start);
            //获取占位符属性值，如${id}, 即获取id
            String placeholder = result.substring(start + this.placeholderPrefix.length(), end);
            //替换整个占位符内容，即将${id}值替换为替换规则回调中的内容
            String replaceContent = placeholder.trim().isEmpty() ? "" : rule.apply(placeholder);
            result.replace(start, end + this.placeholderSuffix.length(), replaceContent);
            start = result.indexOf(this.placeholderPrefix, start + replaceContent.length());
        }
        return result.toString();
    }
    public static String getRandomByConfig(String placeholderValue) {
        StringBuilder sb=new StringBuilder();
        String i = placeholderValue;
        String str=TestReadConfig.map.get(i);
        List<Integer> listIds = Arrays.asList(str.split(",")).stream().map(s -> Integer.parseInt(s.trim())).collect(Collectors.toList());
        if(listIds.get(0).equals(listIds.get(1))){
            return String.valueOf(listIds.get(0));
        }else{
            int randnum1=rand.nextInt(listIds.get(1)-listIds.get(0)) + listIds.get(0);
            String randnum = String.format("%" + listIds.get(2)+ "d",randnum1 );
            return String.valueOf(sb.append(randnum));
        }
    }
    public static String RandomTime(){
        StringBuilder sb=new StringBuilder();
        int hour=rand.nextInt(24);
        String hour1 = String.format("%0" + 2 + "d", hour );
        sb.append(hour1).append(":");
        int min=rand.nextInt(60);
        String min1 = String.format("%0" + 2 + "d", min );
        sb.append(min1).append(":");
        int sec=rand.nextInt(60);
        String sec1 = String.format("%0" + 2 + "d", sec );
        sb.append(sec1).append(".");
        int ran=rand.nextInt(999);
        String ran1 = String.format("%0" + 3 + "d", ran );
        sb.append(ran1);
        return String.valueOf(sb);
    }


}