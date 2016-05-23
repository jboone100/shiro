package com.aerospike.shiro;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.shiro.session.Session;
import org.apache.shiro.session.UnknownSessionException;
import org.apache.shiro.session.mgt.eis.CachingSessionDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Priority;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;

public class AerospikeSessionDAO extends CachingSessionDAO {
	private final String namespace = "test";
	private final String setname = "sessions";
	private final String binname = "data";

	private Integer maxInactiveIntervalInSeconds = 1800;

	private AerospikeClient client;

	private static final transient Logger log = LoggerFactory.getLogger(AerospikeSessionDAO.class);

	public AerospikeSessionDAO() {
		ClientPolicy policy = new ClientPolicy();
		policy.failIfNotConnected = true;
		client = new AerospikeClient(policy, "localhost", 3000);
	}

	public Serializable doCreate(Session session) {
		log.info("Creating a session.");
		String id =  UUID.randomUUID().toString();
		assignSessionId(session, id);
		
		this.storeSession(id, session);
		return id;
	}

	public void doDelete(Session session) {
		log.info("Deleting session " + session.getId());
		Key key = new Key(this.namespace, this.setname, (String)session.getId());
		client.delete(null, key);
	}

	public Session doReadSession(Serializable sessionId) throws UnknownSessionException {
		log.info("Reading session " + (String)sessionId);
		Session session = null;
		
		Key key = new Key(this.namespace, this.setname, (String)sessionId);
		Record rec = client.get(null, key);
		if (rec != null) {
			session = (Session)rec.getValue(binname);
		} else {
			throw new UnknownSessionException();
		}
		
		return session;
	}

	public void doUpdate(Session session) throws UnknownSessionException {
		log.info("Updating session " + (String)session.getId());
		Key key = new Key(this.namespace, this.setname, (String)session.getId());
		Record rec = client.get(null, key);
		if (rec != null) {
			this.storeSession((String)session.getId(), session);
		} else {
			throw new UnknownSessionException();
		}
	}

	public Collection<Session> getActiveSessions() {
		final Set<Session> sessions = new HashSet<Session>();
		
		try {
			ScanPolicy policy = new ScanPolicy();
			policy.concurrentNodes = true;
			policy.priority = Priority.HIGH;
			policy.includeBinData = true;

			client.scanAll(policy, this.namespace, this.setname, new ScanCallback() {
				public void scanCallback(Key key, Record record)
						throws AerospikeException {
					sessions.add((Session)record.getValue("data"));
				}
			}, "data");
		} catch (AerospikeException e) {
			e.printStackTrace();
		}
		
		return sessions;
	}

	private void storeSession(String id, Session session) {
		Key key = new Key(this.namespace, this.setname, id);
		Bin bin = new Bin(binname, session);
		WritePolicy writePolicy = new WritePolicy();
		writePolicy.expiration = maxInactiveIntervalInSeconds;
		client.put(writePolicy, key , bin);
	}
}
