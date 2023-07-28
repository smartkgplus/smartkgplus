package org.linkeddatafragments.datasource;

import com.google.gson.JsonObject;
import java.io.File;
import org.linkeddatafragments.datasource.hdt.HdtDataSource;
import org.linkeddatafragments.exceptions.DataSourceCreationException;
import org.linkeddatafragments.exceptions.UnknownDataSourceTypeException;

import java.io.IOException;

public class DataSourceFactory {

    /**
     * Create a datasource using a JSON config
     *
     * @param config
     * @return datasource interface
     * @throws DataSourceCreationException
     */
    public static IDataSource create(JsonObject config) throws DataSourceCreationException {
        String title = config.getAsJsonPrimitive("title").getAsString();
        String description = config.getAsJsonPrimitive("description").getAsString();
        String typeName = config.getAsJsonPrimitive("type").getAsString();
        JsonObject settings = config.getAsJsonObject("settings");

        final IDataSourceType type = DataSourceTypesRegistry.getType(typeName);
        System.out.println("Type ====> " + type.toString());
        if (type == null) {
            throw new UnknownDataSourceTypeException(typeName);
        }

        // System.out.println("After the execption ====> " + type.toString());
        return type.createDataSource(title, description, settings);
    }

    public static IDataSource createMoleculeDatasource(String path) throws IOException {
        System.err.println("PAAAAAAATH:" + path);

         
          //  System.err.println("PAAAAAAATH after editing:" + path.substring(0, path.lastIndexOf("_")));
        File file = new File(path);
        if (!file.exists() || file.isDirectory()) {
            path = path.substring(0, path.lastIndexOf("_"));
            System.err.println("PAAAAAAATH after editing:" + path);
            return new HdtDataSource(path, "Molecule HDT file", path);
        }
        
       
        return new HdtDataSource(path, "Molecule HDT file", path);
    }

}
