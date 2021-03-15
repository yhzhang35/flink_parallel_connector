package com.yunhai.learn.connector;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import static org.apache.poi.ss.usermodel.Row.MissingCellPolicy.RETURN_NULL_AND_BLANK;

/**
 * @function: excel的并行读取的连接类
 * @author: yhzhang
 * @create: 2021-03-10 22:33
 **/
public class XLSConnector extends RichParallelSourceFunction<String> {
    // 总的行数
    private long totalNumber;
    // 读取的sheet的索引
    private int sheetIndex = 0;
    // 指定的索引
    private Sheet sheet;
    // 指定工作簿
    private Workbook wb;
    // 文件名
    private String filepath;


    @Override
    public void open(Configuration parameters) throws Exception {
        Logger logger = LoggerFactory.getLogger(this.getClass());

        // 将对应的文件路径变成一个工作簿
        if (filepath == null) {
            return;
        }
        // 初始化工作簿
        if (wb==null) {
            String ext = filepath.substring(filepath.lastIndexOf("."));
            try {
                InputStream is = new FileInputStream(filepath);
                if (".xls".equals(ext)) {
                    wb = new HSSFWorkbook(is);
                } else if (".xlsx".equals(ext)) {
                    wb = new XSSFWorkbook(is);
                } else {
                    wb = null;
                }
            } catch (FileNotFoundException e) {
                logger.error("FileNotFoundException", e);
            } catch (IOException e) {
                logger.error("IOException", e);
            }
        }

        // 获取指定的sheet
        sheet=wb.getSheetAt(sheetIndex);

        // 获取总的数据行数
        totalNumber=sheet.getLastRowNum();

        // 父类需要配置的一些参数
        super.open(parameters);

    }

    @Override
    public void run(SourceContext ctx) throws Exception {
        // 读取相应的数据
        // 获取上下文信息
        int totalTasks=getRuntimeContext().getNumberOfParallelSubtasks();
        int currentIndex=getRuntimeContext().getIndexOfThisSubtask();

        // 计算当前线程应该处理的数据范围
        long start=totalNumber/totalTasks*currentIndex;
        long end=currentIndex==totalTasks-1?totalNumber:totalNumber/totalTasks*(currentIndex+1);
        System.out.println(String.format("总的数量为：%d,总的并行度为：%d，当前线程索引：%d,负责的数据段：%d-%d", totalNumber, totalTasks,currentIndex, start, end));

        // 开始处理数据
        Row row;
        int columns=0;
        StringBuilder sb=new StringBuilder();
        for (long i=start;i<end;i++){
            //System.out.println(String.format("处理第%d行", i));
            sb.delete(0,sb.length());
            row=sheet.getRow((int)i);
            columns=row.getPhysicalNumberOfCells();
            for (int j=0;j<columns;j++){
                Cell cell=row.getCell(j);
                if (cell!=null)
                    sb.append(row.getCell(j).toString()+"\t");
                else
                    sb.append("\t");
            }

            // 收集处理好的数据
            //System.out.println(String.format("第%d行的数据为：%s", i, sb.toString()));
            ctx.collect(sb.toString());
        }

    }


    @Override
    public void cancel() {

    }

    // 构造方法
    public XLSConnector( int sheetIndex, String filepath) {
        this.sheetIndex = sheetIndex;
        this.filepath = filepath;
    }


    // 测试connector
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(8);
        DataStream<String> source=env.addSource(new XLSConnector(1,"C:\\新建文件夹\\附件2：2020年8月综合技能岗复习题库.xls"));
        //env.setParallelism(8);
        System.out.println(env.getParallelism());
        //source.print();
        source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return !StringUtils.isEmpty(value);
            }
        }).map(new MapFunction<String, String>() {

            @Override
            public String map(String value) throws Exception {
                return value.split("\t")[1];
            }
        }).print();
        env.execute("测试excel的连接器");
    }
}
