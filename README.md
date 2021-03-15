# Flink并行读取Source数据

## 一  ExcelConnector(XLSConnector,XLSLConnector)--Flink并行读取excel

> 引言： 在大数据环境下，什么都讲究效率，如何并行处理数据，存储数据变得越来越重要。在很早之前我处理excel(xls,xlsx格式)的数据都是用的python的pandas来实现，这样做的好处是python API的简单易用。但是后来我就发现了一个问题，当处理大的数据时，就不能并行处理，是否可以结合大数据框架（Flink,Spark）来实现结构化数据的并行处理。但是在处理前，面临一个问题，如何高效并行的把数据读取到内存中变成了一个重要因素，因为一旦数据变成了大数据环境下的数据结构，那么就可以对它进行并行处理，前提就是让他如何快速”变身“。结合RichParallelSourceFunction，我实现了自定义并行度读取Excel数据，为高效处理Excel数据提供了一个基础。话不多说下面讲解核心思路，具体完整的代码：https://github.com/yhzhang35/flink_parallel_connector/blob/master/XLSConnector.java

Flink的source接口有：

* SourceFunction
* RichSourceFunction
* parallelSourceFunction
* RichParallelSourceFunction

比较推荐用Rich开头的接口，因为能获取上下文的信息。而SourceFunction和RichSourceFunction是单并行度的接口。不能手动设置并行度。



**配置open函数，一般是配置每个task连接数据源的参数。**

```java
// open函数的重写，open函数是每个并行度开始执行时首先执行的配置函数
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
```



**下面是核心代码，通过将数据分段给每个SubTask来实现并行读取**

```java
@Override
public void run(SourceContext ctx) throws Exception {
        // 读取相应的数据
        // 获取上下文信息
        int totalTasks=getRuntimeContext().getNumberOfParallelSubtasks();
        int currentIndex=getRuntimeContext().getIndexOfThisSubtask();

        // 计算当前线程应该处理的数据范围,代码的核心在此处，通过将总的数据进行分段并行处理，每个subtask负责一个范围。
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
```





> 参考文档：
>
> * 利用java Api读取Excel数据。https://blog.csdn.net/lipr86/article/details/79316630
> * Flink官网解读RichParallelSourceFunction https://ci.apache.org/projects/flink/flink-docs-master/api/java/org/apache/flink/streaming/api/functions/source/RichParallelSourceFunction.html
> * Flink并行读取Mysql数据 https://www.codeleading.com/article/26134344305/

