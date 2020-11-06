package org.hibernate.jpa.test.lock;

import org.hibernate.jpa.test.BaseEntityManagerFunctionalTestCase;
import org.hibernate.test.legacy.Abstract;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Generated;
import javax.persistence.*;

import static org.hibernate.testing.transaction.TransactionUtil.doInJPA;

public class TablespanLockTest extends BaseEntityManagerFunctionalTestCase {

	Subclass subclass;

	@Before
	public void setUp() throws Exception {
		doInJPA( this::entityManagerFactory, em -> {
			subclass = new Subclass();
			subclass.field = "Field value";
			em.persist(subclass);
		});
	}

	@Test
	public void testPessimisticLock() {
		doInJPA( this::entityManagerFactory, em -> {
			em.find(AbstractEntity.class, subclass.id, LockModeType.PESSIMISTIC_WRITE);
		});
	}

	@Override
	protected Class<?>[] getAnnotatedClasses() {
		return new Class<?>[] { AbstractEntity.class, Subclass.class };
	}

	@Entity(name = "A")
	@Inheritance(strategy = InheritanceType.JOINED)
	public static class AbstractEntity {

		@Id
		@GeneratedValue(strategy = GenerationType.AUTO)
		Long id;

	}

	@Entity(name = "B")
	public static class Subclass extends AbstractEntity {

		String field;

	}

}
