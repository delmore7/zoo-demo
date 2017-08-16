import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class Main {

	public static final String LEADER = "/leader/";

	public static ZooKeeper zk;

	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
		zk = new ZooKeeper("localhost:2181", 60000 * 10, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				System.out.println("已经触发了" + event.getType() + "事件！");
			}
		});
		String node = createNode(zk);
		System.out.println(node);
		electionNoWaiting(node);
		Thread.sleep(100000);
		
		
		 
	}

	private static String createNode(ZooKeeper zk) throws KeeperException, InterruptedException {
		return zk.create(LEADER, "leader election...".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
				CreateMode.EPHEMERAL_SEQUENTIAL);
	}

	private static String preNode(String node) throws KeeperException, InterruptedException {
		String currentNode = node.replace(LEADER, "");
		Integer preNum = Integer.valueOf(currentNode) - 1;

		if (preNum == 0)
			return "";
		String preNode = StringUtils.repeat("0", 10 - ("" + preNum).length()) + preNum;
		System.out.println("pre node: " + LEADER + preNode);

		Stat stat = exists(LEADER + preNode);
		if (!amILeader(node.replace(LEADER, "")) && stat == null) {
			return preNode(preNode);
		} else {
			return preNode;
		}
	}

	/**
	 * 一个阻塞直到获得leadership
	 *
	 * @param node
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	private static void election(final String node) throws KeeperException, InterruptedException {

		final CountDownLatch connectedSignal = new CountDownLatch(1);

		if (amILeader(node.replace(LEADER, ""))) {
			System.out.println("I'm leader now..." + node);
			connectedSignal.countDown();
		} else {
			String preNode = preNode(node);
			System.out.println(node + " will be watching :" + preNode);
			// 监听前一节点状态
			zk.getData(LEADER + preNode, new Watcher() {
				@Override
				public void process(WatchedEvent event) {
					System.out.println(event);
					try {
						// 前节点挂掉，再次选举
						if (event.getType().equals(Event.EventType.NodeDeleted)) {
							//connectedSignal.countDown();
							election(node);
						}
					} catch (KeeperException | InterruptedException e) {
						System.out.println(e.getMessage());
					}
				}
			}, null);
		}

		connectedSignal.await();
	}

	/**
	 * 获得leadership返回true
	 * @param node
	 * @return
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	private static Boolean electionNoWaiting(final String node) throws KeeperException, InterruptedException {

		if (amILeader(node.replace(LEADER, ""))) {
			System.out.println("I'm leader now..." + node);
			return Boolean.TRUE;
		} else {
			String preNode = preNode(node);
			System.out.println(node + " will be watching :" + preNode);
			// 监听前一节点状态
			zk.getData(LEADER + preNode, new Watcher() {
				@Override
				public void process(WatchedEvent event) {
					System.out.println(event);
					try {
						// 前节点挂掉，再次选举
						if (event.getType().equals(Event.EventType.NodeDeleted)) {
							electionNoWaiting(node);
						}
					} catch (KeeperException | InterruptedException e) {
						System.out.println(e.getMessage());
					}
				}
			}, null);
		}

		return Boolean.FALSE;
	}

	private static Stat exists(String path) throws KeeperException, InterruptedException {
		return zk.exists(path, true);
	}

	private static List<String> allChildren() throws KeeperException, InterruptedException {
		List<String> children = zk.getChildren(LEADER.substring(0, LEADER.length() - 1), false);
		System.out.println(children);
		return children;
	}

	private static String min(List<String> children) {
		if (children.size() > 0) {
			int min = children.stream().mapToInt(s -> Integer.valueOf(s)).min().getAsInt();
			String minNode = StringUtils.repeat("0", 10 - ("" + min).length()) + min;
			return minNode;
		}
		return null;
	}

	/**
	 *
	 * @param node
	 * @return
	 */
	public static Boolean amILeader(String node) throws KeeperException, InterruptedException {
		String min = min(allChildren());
		if (node.equals(min)) {
			return true;
		}
		return false;
	}

}