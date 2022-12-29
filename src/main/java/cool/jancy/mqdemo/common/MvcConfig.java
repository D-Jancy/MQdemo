package cool.jancy.mqdemo.common;

import org.springframework.boot.SpringBootConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.web.servlet.config.annotation.*;
import org.springframework.web.servlet.view.InternalResourceViewResolver;

/**
 * @author dengjie
 * @version 1.0
 * @ClassName : MvcConfig
 * @description: TODO
 * @date 2022/03/14 14:27
 */
@SpringBootConfiguration
@EnableWebMvc
public class MvcConfig implements WebMvcConfigurer {

    /**
     * 配置请求视图映射
     *
     * @return
     */
    @Bean
    public InternalResourceViewResolver ForeresourceViewResolver() {
        InternalResourceViewResolver internalResourceViewResolver = new InternalResourceViewResolver();
        //请求视图文件的前缀地址
        internalResourceViewResolver.setPrefix("/");
        //请求视图文件的后缀
        internalResourceViewResolver.setSuffix(".html");
        return internalResourceViewResolver;
    }

    @Override
    public void configureViewResolvers(ViewResolverRegistry registry) {
        WebMvcConfigurer.super.configureViewResolvers(registry);
        registry.viewResolver(ForeresourceViewResolver());
    }


    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        WebMvcConfigurer.super.addResourceHandlers(registry);
        registry.addResourceHandler("/**")
                .addResourceLocations("classpath:/static/")
                .addResourceLocations("classpath:/templates/");
    }

    @Override
    public void addViewControllers(ViewControllerRegistry registry) {
        WebMvcConfigurer.super.addViewControllers(registry);
        registry.addViewController("/").setViewName("index");
    }

    //
    ///**
    // * 添加到消息转换器
    // *
    // * @param converters
    // */
    //@Override
    //public void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
    //    //如果放在jacson下面会出现字符串多一个的问题
    //    converters.add(stringHttpMessageConverter());
    //    converters.add(jackson2HttpMessageConverter());
    //}
    //
    ///**
    // * 处理返回json格式
    // * 定义时间格式转换器
    // *
    // * @return MappingJackson2HttpMessageConverter
    // */
    //@Bean
    //public MappingJackson2HttpMessageConverter jackson2HttpMessageConverter() {
    //    MappingJackson2HttpMessageConverter converter = new MappingJackson2HttpMessageConverter();
    //    ObjectMapper mapper = new ObjectMapper();
    //    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    //    //主要是这句处理
    //    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    //    //时间格式化
    //    mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
    //    converter.setObjectMapper(mapper);
    //
    //    List<MediaType> list = Arrays.asList(
    //            MediaType.APPLICATION_JSON,
    //            MediaType.ALL);//防止spring boot admin报错(No converter for [class org.springframework.boot.actuate.health.SystemHealth]with preset Content - Type 'null')
    //    converter.setSupportedMediaTypes(list);
    //    return converter;
    //}
    //
    //@Bean
    //public StringHttpMessageConverter stringHttpMessageConverter() {
    //    return new StringHttpMessageConverter();
    //}
}
