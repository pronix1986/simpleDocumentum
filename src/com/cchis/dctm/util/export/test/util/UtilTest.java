package com.cchis.dctm.util.export.test.util;

import static com.cchis.dctm.util.export.util.ExportConstants.ZIP_EXT;
import static org.junit.jupiter.api.Assertions.*;

import com.cchis.dctm.util.export.util.Util;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.attribute.FileTime;
import java.util.concurrent.TimeUnit;

@DisplayName("Util")
class UtilTest {

    @BeforeEach
    void setUp() {
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    @Tag("fast")
    void toISODate() {
       //yyyy-MM-dd'T'HH:mm:ss
        FileTime ft = FileTime.from(17897L, TimeUnit.DAYS);
        assertEquals(Util.toISODate(ft), "2019-01-01T03:00:00");
    }


    @Test
    void wrap() {
        assertEquals(Util.wrap("Total", " --- "), " --- Total --- ");
    }

    @Test
/*    @ParameterizedTest
    @ValueSource(strings = {"_bak", "_BAK"})*/
    void trimEnd(/*String bak*/) {
        String origPathStr = "d:/Temp/test.zip";
        String bak = "_bak";
        String backupPathStr = Util.trimEnd(origPathStr, ZIP_EXT) + bak + ZIP_EXT;

        assertEquals(backupPathStr, "d:/Temp/test_bak.zip");
    }

}