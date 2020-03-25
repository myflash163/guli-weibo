package com.atguigu.test;

import com.atguigu.contants.Constants;
import com.atguigu.dao.HBaseDao;
import com.atguigu.utils.HBaseUtil;

import java.io.IOException;

public class TestWeiBo {
    public static void init() throws IOException {
        //创建命名空间
        HBaseUtil.createNameSpace(Constants.NAMESPACE);
        //创建微博内容表
        HBaseUtil.createTable(Constants.CONTENT_TABLE, Constants.CONTENT_TABLE_VERSIONS,
                Constants.CONTENT_TABLE_CF);
        //创建用户关系表
        HBaseUtil.createTable(Constants.RELATION_TABLE, Constants.RELATION_TABLE_VERSIONS,
                Constants.RELATION_TABLE_CF1, Constants.RELATION_TABLE_CF2);
        //创建收件箱表
        HBaseUtil.createTable(Constants.INBOX_TABLE, Constants.INBOX_TABLE_VERSIONS,
                Constants.INBOX_TABLE_CF);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        init();
        //1001发布微博
        HBaseDao.publishWeibo("1001", "赶紧下课吧!!!");
        //1002关注1001和1003
        HBaseDao.addAttends("1002", "1001", "1003");
        //获取1002初始化页面
        HBaseDao.getInit("1002");
        System.out.println("***********111***********");
        //1003发布3条微博，同时1001 发布2条微博
        HBaseDao.publishWeibo("1003", "说的赶紧下课！！");
        Thread.sleep(10);
        HBaseDao.publishWeibo("1001", "我没说话！！");
        Thread.sleep(10);
        HBaseDao.publishWeibo("1003", "那谁说的！！");
        Thread.sleep(10);
        HBaseDao.publishWeibo("1001", "反正飞机是下线了！！");
        Thread.sleep(10);
        HBaseDao.publishWeibo("1003", "你们爱咋咋地！！");
        Thread.sleep(10);
        //获取1002初始化页面
        HBaseDao.getInit("1002");
        System.out.println("***********222***********");
        //1002取消关注1003
        HBaseDao.deleteAttends("1002", "1003");
        //获取1002初始化页面
        HBaseDao.getInit("1002");
        System.out.println("***********333***********");
        //1002再次关注1003
        HBaseDao.addAttends("1002", "1003");
        //获取1002初始化页面
        HBaseDao.getInit("1002");
        System.out.println("***********444***********");
        //获取1001微博详情
        HBaseDao.getWeiBo("1001");


    }
}
