/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */

package org.hibernate.test.hql;

import org.hibernate.annotations.NaturalId;
import org.hibernate.hql.spi.FilterTranslator;
import org.hibernate.query.Query;
import org.hibernate.testing.junit4.BaseCoreFunctionalTestCase;
import org.junit.Before;
import org.junit.Test;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import java.util.Collections;
import java.util.List;

import static org.hibernate.testing.transaction.TransactionUtil.doInHibernate;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Jan-Willem Gmelig Meyling
 * @author Christian Beikov
 */
public class NaturalIdDereferenceTest extends BaseCoreFunctionalTestCase {

    @Override
    protected Class<?>[] getAnnotatedClasses() {
        return new Class[] { Book.class, BookRef.class, BookRefRef.class };
    }

    @Before
    public void setUp() {
        doInHibernate( this::sessionFactory, session -> {
            Book book = new Book();
            book.isbn = "abcd";
            session.persist(book);

            BookRef bookRef = new BookRef();
            bookRef.naturalBook = bookRef.normalBook = book;
            session.persist(bookRef);

            session.flush();
            session.clear();
        } );
    }

    @Test
    public void naturalIdDereferenceTest() {
        doInHibernate(this::sessionFactory, session -> {
            Query query = session.createQuery("SELECT r.normalBook.isbn FROM BookRef r");
            List resultList = query.getResultList();
            assertFalse(resultList.isEmpty());
            assertEquals(1, getSQLJoinCount(query));
        });
    }


    @Test
    public void naturalIdDereferenceTest2() {
        doInHibernate(this::sessionFactory, session -> {
            Query query = session.createQuery("SELECT r.naturalBook.isbn FROM BookRef r");
            List resultList = query.getResultList();
            assertFalse(resultList.isEmpty());
            assertEquals(0, getSQLJoinCount(query));
        });
    }

    @Test
    public void naturalIdDereferenceTest3() {
        doInHibernate(this::sessionFactory, session -> {
            Query query = session.createQuery("SELECT r2.normalBookRef.normalBook.isbn FROM BookRefRef r2");
            query.getResultList();
            assertEquals(2, getSQLJoinCount(query));
        });
    }

    @Test
    public void naturalIdDereferenceTest4() {
        doInHibernate(this::sessionFactory, session -> {
            Query query = session.createQuery("SELECT r2.naturalBookRef.normalBook FROM BookRefRef r2");
            query.getResultList();
            assertEquals(2, getSQLJoinCount(query));
        });
    }

    @Test
    public void naturalIdDereferenceTest5() {
        doInHibernate(this::sessionFactory, session -> {
            Query query = session.createQuery("SELECT r2.normalBookRef.normalBook FROM BookRefRef r2");
            query.getResultList();
            assertEquals(2, getSQLJoinCount(query));
        });
    }


    @Test
    public void naturalIdDereferenceTest6() {
        doInHibernate(this::sessionFactory, session -> {
            Query query = session.createQuery("SELECT r2.normalBookRef.normalBook.id, r3.naturalBookRef.naturalBook.isbn FROM BookRefRef r2 JOIN BookRefRef r3 ON r2.normalBookRef.normalBook.isbn = r3.naturalBookRef.naturalBook.isbn");
            query.getResultList();
            assertEquals(3, getSQLJoinCount(query));
        });
    }


    @Test
    public void naturalIdDereferenceTest7() {
        doInHibernate(this::sessionFactory, session -> {
            Query query = session.createQuery("SELECT b.normalBook FROM BookRefRef a JOIN BookRef b ON b.naturalBook.isbn = a.naturalBookRef.naturalBook.isbn");
            query.getResultList();
            assertEquals(2, getSQLJoinCount(query));
        });
    }

    @Test
    public void naturalIdDereferenceTest8() {
        doInHibernate(this::sessionFactory, session -> {
            Query query = session.createQuery("SELECT r2.normalBookRef.normalBook.id FROM BookRefRef r2");
            query.getResultList();
            assertEquals(1, getSQLJoinCount(query));
        });
    }


    @Test
    public void naturalIdDereferenceTest9() {
        doInHibernate(this::sessionFactory, session -> {
            Query query = session.createQuery("SELECT r2.naturalBookRef.naturalBook.id FROM BookRefRef r2");
            query.getResultList();
            assertEquals(1, getSQLJoinCount(query));
        });
    }

    @Test
    public void naturalIdDereferenceTest10() {
        doInHibernate(this::sessionFactory, session -> {
            // Query fails prior to optimization, due to implicit join in on clause, so simply passing of the test is sufficient
            Query query = session.createQuery("SELECT r.normalBook.isbn FROM BookRef r JOIN r.normalBook b ON b.isbn = r.normalBook.isbn");
            query.getResultList();
            assertEquals(1, getSQLJoinCount(query));
        });
    }

    private int getSQLJoinCount(Query query) {
        String sqlQuery = getSQLQuery(query).toLowerCase();

        int lastIndex = 0;
        int count = 0;

        while (lastIndex != -1) {

            lastIndex = sqlQuery.indexOf( " join ", lastIndex );

            if (lastIndex != -1) {
                count++;
                lastIndex += " join ".length();
            }
        }
        
        return count;
    }

    private String getSQLQuery(Query query) {
        FilterTranslator naturalIdJoinGenerationTest1 = this.sessionFactory().getSettings().getQueryTranslatorFactory().createFilterTranslator(
                "nid",
                query.getQueryString(),
                Collections.emptyMap(),
                this.sessionFactory());
        naturalIdJoinGenerationTest1.compile(Collections.emptyMap(), false);
        return naturalIdJoinGenerationTest1.getSQLString();
    }

    @Override
    protected boolean isCleanupTestDataRequired() {
        return true;
    }

    @Override
    protected boolean isCleanupTestDataUsingBulkDelete() {
        return true;
    }

    @Entity(name = "Book")
    @Table(name = "book")
    public static class Book {

        @Id
        @GeneratedValue(strategy = GenerationType.AUTO)
        private Long id;

        @NaturalId
        @Column(name = "isbn", unique = true)
        private String isbn;

    }

    @Entity(name = "BookRef")
    @Table(name = "bookref")
    public static class BookRef {

        @Id
        @GeneratedValue(strategy = GenerationType.AUTO)
        private Long id;

        @ManyToOne
        @JoinColumn(nullable = true)
        private Book normalBook;

        @ManyToOne
        @JoinColumn(name = "isbn", referencedColumnName = "isbn")
        private Book naturalBook;

    }

    @Entity(name = "BookRefRef")
    @Table(name = "bookrefref")
    public static class BookRefRef {

        @Id
        @GeneratedValue(strategy = GenerationType.AUTO)
        private Long id;

        @ManyToOne
        @JoinColumn(nullable = true)
        private BookRef normalBookRef;

        @OneToOne
        @JoinColumn(name = "isbn", referencedColumnName = "isbn")
        private BookRef naturalBookRef;
    }

}
