package com.migu.schedule;


import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.TaskInfo;

import java.util.*;

/*
*类名和方法不能修改
 */
public class Schedule {

    //定义节点注册队列
    private  static List<Integer> nodeIds = new LinkedList<Integer>();
    //定义任务挂起队列
    private  static List<Integer> taskIds = new LinkedList<Integer>();
    //定义任务信息列表
    private  static List<TaskInfo> tasks = new ArrayList<TaskInfo>();
    //定义任务资源消耗表
    private  static Map<Integer,Integer> taskConsum = new HashMap<Integer, Integer>();

    /**
     * 系统初始化
     * @return
     */
    public int init() {
        // 清空任务列表,注册节点和任务挂起队列
        if(tasks.size()!=0){
            tasks.clear();
        }
        if(taskIds.size()!=0){
            taskIds.clear();
        }
        if(nodeIds.size()!=0){
            nodeIds.clear();
        }
        return ReturnCodeKeys.E001;
    }

    /**
     * 注册节点
     * @param nodeId
     * @return
     */
    public int registerNode(int nodeId) {
        if(nodeId<=0){
            return ReturnCodeKeys.E004;
        }
        if(nodeIds.contains(nodeId)){
            return ReturnCodeKeys.E005;
        }
        nodeIds.add(nodeId);
        return ReturnCodeKeys.E003;
    }

    /**
     * 注销节点
     * @param nodeId
     * @return
     */
    public int unregisterNode(int nodeId) {
        if(nodeId<=0){
            return ReturnCodeKeys.E004;
        }
        if(!nodeIds.contains(nodeId)){
            return ReturnCodeKeys.E007;
        }
        //注销节点
        if(tasks.size()!=0)
        {
            for (TaskInfo taskInfo : tasks)
            {
                if (taskInfo.getNodeId() == nodeId)
                {
                    //移到任务挂起队列
                    taskIds.add(taskInfo.getTaskId());
                    tasks.remove(taskInfo);
                }
            }
        }
        for(int i=nodeIds.size()-1;i>=0;i--){
            if(nodeIds.get(i)==nodeId){
                nodeIds.remove(i);
            }
        }
        return ReturnCodeKeys.E006;
    }

    /**
     * 添加任务
     * @param taskId
     * @param consumption
     * @return
     */
    public int addTask(int taskId, int consumption) {
        if(taskId<=0)
        {
            return ReturnCodeKeys.E009;
        }
        if(taskIds.contains(taskId))
        {
            return ReturnCodeKeys.E010;
        }
        taskConsum.put(taskId,consumption);
        //移到任务挂起队列
        taskIds.add(taskId);
        return ReturnCodeKeys.E008;
    }

    /**
     * 删除任务
     * @param taskId
     * @return
     */
    public int deleteTask(int taskId) {
        if(taskId<=0)
        {
            return ReturnCodeKeys.E009;
        }
        if(!taskIds.contains(taskId))
        {
            return ReturnCodeKeys.E012;
        }

        for(int i=taskIds.size()-1;i>=0;i--)
        {
            if(taskIds.get(i)==taskId){
                taskIds.remove(i);
            }
        }
        return ReturnCodeKeys.E011;
    }

    /**
     * 任务调度
     * @param threshold
     * @return
     */
    public int scheduleTask(int threshold) {


        return ReturnCodeKeys.E000;
    }

    /**
     * 查询任务调度列表
     * @param tasks
     * @return
     */
    public int queryTaskStatus(List<TaskInfo> tasks) {
        if(tasks == null || tasks.size()==0){
            return ReturnCodeKeys.E016;
        }
        TreeMap<Integer,Integer> map = new TreeMap<Integer, Integer>();
        for(TaskInfo taskInfo : tasks){
            map.put(taskInfo.getTaskId(),taskInfo.getNodeId());
        }
        tasks.clear();
        List<Map> list = new ArrayList<Map>();
        
        return ReturnCodeKeys.E015;
    }

}
