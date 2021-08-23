package iot.meters.data.generator;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CSV {

    static final Logger log = LoggerFactory.getLogger(CSV.class);

    static final int NUMMARK = 10;
    static final char DQUOTE = '"';

    char separator;

    public CSV(char separator) {
        this.separator = separator;
    }

    public List<String> parse(String line) {
        List<String> fields = new ArrayList<>();
        StringReader reader = new StringReader(line);
        try {
            StringBuilder sb = new StringBuilder();
            int value;
            while ((value = reader.read()) != -1) {
                char c = (char) value;
                switch (c) {
                    case DQUOTE: {
                        while ((value = reader.read()) != -1) {
                            c = (char) value;
                            if (c == DQUOTE) {
                                reader.mark(NUMMARK);
                                if ( (value = reader.read()) == -1 ) {
                                    fields.add(sb.toString());
                                    sb.delete(0, sb.length());
                                    return fields;
                                } else if ( (c = (char)value) == DQUOTE ) {
                                    sb.append(DQUOTE);
                                } else {
                                    reader.reset();
                                    break;
                                }
                            } else {
                                sb.append(c);
                            }
                        }
                        if (value == -1) {
                            fields.add(sb.toString());
                            return fields;
                        }
                    }
                    break;

                    default:
                        if (c == separator) {
                            fields.add(sb.toString());
                            sb.delete(0, sb.length());
                        } else {
                            sb.append(c);
                        }

                }
            }
            if (sb.length() > 0) {
                fields.add(sb.toString());
                return fields;
            }
        } catch (IOException e) {
            log.error("Exception parsing line '" + line + "'. Ignoring");
            return null;
        }
        return fields;
    }

}
