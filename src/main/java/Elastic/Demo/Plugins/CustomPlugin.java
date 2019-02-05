package Elastic.Demo.Plugins;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.Plugin;

public class CustomPlugin extends Plugin {
  
  @Override
  public String description()
  {
    return "Elasticsearch plugin for demo";
  }
  
  @Override
  public String name()
  {
    return "demo-plugin";
  }
  
  @Override
  public Collection<Module> nodeModules()
  {
    System.out.println("node moduled called");
    List<Module> modules = new ArrayList<Module>();
    modules.add(new ModuleConfiguration());
    return modules;
  }
  
  public static class ModuleConfiguration extends AbstractModule {
    
    @Override
    protected void configure()
    {
      System.out.println("node moduledconfig called");
      bind(Apartment.class).asEagerSingleton();
    }
  }
  
}
