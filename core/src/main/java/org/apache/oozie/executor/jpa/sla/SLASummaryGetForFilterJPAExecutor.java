/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.oozie.executor.jpa.sla;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import org.apache.oozie.BundleActionBean;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.event.SLAEvent;
import org.apache.oozie.client.event.SLAEvent.EventStatus;
import org.apache.oozie.executor.jpa.JPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.servlet.XServletException;
import org.apache.oozie.sla.SLASummaryBean;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.Pair;
import org.apache.oozie.util.XLog;

import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Order;
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import javax.persistence.criteria.Subquery;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.Metamodel;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Load the list of SLASummaryBean (for dashboard) and return the list.
 */
public class SLASummaryGetForFilterJPAExecutor implements JPAExecutor<List<SLASummaryBean>> {

    private static final String DBFIELD_EXPECTED_START_TS = "expectedStartTS";
    private static final String DBFIELD_ACTUAL_START_TS = "actualStartTS";
    private static final String DBFIELD_EXPECTED_DURATION = "expectedDuration";
    private static final String DBFIELD_ACTUAL_DURATION = "actualDuration";
    private static final String DBFIELD_EXPECTED_END_TS = "expectedEndTS";
    private static final String DBFIELD_ACTUAL_END_TS = "actualEndTS";
    private static final String DBFIELD_EVENT_STATUS = "eventStatus";
    private static final String DBFIELD_CREATED_TIME_TS = "createdTimeTS";
    private static final String DBFIELD_NOMINAL_TIME_TS = "nominalTimeTS";
    private static final String DBFIELD_JOB_STATUS = "jobStatus";
    private static final String DBFIELD_USER = "user";
    private static final String DBFIELD_APP_TYPE = "appType";
    private static final String DBFIELD_APP_NAME = "appName";
    private static final String DBFIELD_SLA_STATUS = "slaStatus";
    private static final String DBFIELD_JOB_ID = "jobId";
    private static final String DBFIELD_PARENT_ID = "parentId";
    private static final String DBFIELD_BUNDLE_ID = "bundleId";
    private static final String DBFIELD_ID = "id";
    private static final String DBFIELD_COORD_ID = "coordId";
    private static final String DEFAULT_SORTBY_COLUMN = DBFIELD_NOMINAL_TIME_TS;
    @VisibleForTesting
    final FilterCollection filterCollection;
    private int numMaxResults;
    private List<String> possibleSortbyColumns;
    private String sortbyColumn;
    private boolean isDescendingOrder;
    private static XLog LOG = XLog.getLog(SLASummaryGetForFilterJPAExecutor.class);
    private CriteriaBuilder criteriaBuilder;
    private CriteriaQuery<SLASummaryBean> criteriaQuery;
    private Root<SLASummaryBean> root;
    private List<Predicate> preds;
    private static final String multiValueSeparator = ",";
    /**
     * Matches a bundle_id which has the ${padded_counter}-${startTime}-${systemId}-B format,
     * where padded_counter is exactly 7 digits, startTime is exactly 15 digits and
     * systemId is at most 10 characters like {@code 1234567-150130225116604-oozie-B}
     *
     * {@link UUIDService#generateId(UUIDService.ApplicationType)}
     */
    private static final Pattern BUNDLE_ID_PATTERN = Pattern.compile("\\d{7}-\\d{15}-.{1,10}-B$");

    private enum FilterComparator {
        LIKE, EQUALS, GREATER_OR_EQUALS, LESSTHAN_OR_EQUALS, IN
    }

    private enum FilterField {
        APP_NAME(String.class, DBFIELD_APP_NAME, FilterComparator.LIKE),
        APP_TYPE(String.class, DBFIELD_APP_TYPE, FilterComparator.LIKE),
        USER_NAME(String.class, DBFIELD_USER, FilterComparator.LIKE),
        JOB_STATUS(String.class, DBFIELD_JOB_STATUS, FilterComparator.LIKE),
        NOMINAL_START(Timestamp.class, DBFIELD_NOMINAL_TIME_TS, FilterComparator.GREATER_OR_EQUALS),
        NOMINAL_END(Timestamp.class, DBFIELD_NOMINAL_TIME_TS, FilterComparator.LESSTHAN_OR_EQUALS),
        NOMINAL_AFTER(Timestamp.class, DBFIELD_NOMINAL_TIME_TS, FilterComparator.GREATER_OR_EQUALS),
        NOMINAL_BEFORE(Timestamp.class, DBFIELD_NOMINAL_TIME_TS, FilterComparator.LESSTHAN_OR_EQUALS),
        CREATED_AFTER(Timestamp.class, DBFIELD_CREATED_TIME_TS, FilterComparator.GREATER_OR_EQUALS),
        CREATED_BEFORE(Timestamp.class, DBFIELD_CREATED_TIME_TS, FilterComparator.LESSTHAN_OR_EQUALS),
        EXPECTEDSTART_AFTER(Timestamp.class, DBFIELD_EXPECTED_START_TS, FilterComparator.GREATER_OR_EQUALS),
        EXPECTEDSTART_BEFORE(Timestamp.class, DBFIELD_EXPECTED_START_TS, FilterComparator.LESSTHAN_OR_EQUALS),
        EXPECTEDEND_AFTER(Timestamp.class, DBFIELD_EXPECTED_END_TS, FilterComparator.GREATER_OR_EQUALS),
        EXPECTEDEND_BEFORE(Timestamp.class, DBFIELD_EXPECTED_END_TS, FilterComparator.LESSTHAN_OR_EQUALS),
        ACTUALSTART_AFTER(Timestamp.class, DBFIELD_ACTUAL_START_TS, FilterComparator.GREATER_OR_EQUALS),
        ACTUALSTART_BEFORE(Timestamp.class, DBFIELD_ACTUAL_START_TS, FilterComparator.LESSTHAN_OR_EQUALS),
        ACTUALEND_AFTER(Timestamp.class, DBFIELD_ACTUAL_END_TS, FilterComparator.GREATER_OR_EQUALS),
        ACTUALEND_BEFORE(Timestamp.class, DBFIELD_ACTUAL_END_TS, FilterComparator.LESSTHAN_OR_EQUALS),
        ACTUAL_DURATION_MIN(Integer.class, DBFIELD_ACTUAL_DURATION, FilterComparator.GREATER_OR_EQUALS),
        ACTUAL_DURATION_MAX(Integer.class, DBFIELD_ACTUAL_DURATION, FilterComparator.LESSTHAN_OR_EQUALS),
        EXPECTED_DURATION_MIN(Integer.class, DBFIELD_EXPECTED_DURATION, FilterComparator.GREATER_OR_EQUALS),
        EXPECTED_DURATION_MAX(Integer.class, DBFIELD_EXPECTED_DURATION, FilterComparator.LESSTHAN_OR_EQUALS),
        SLA_STATUS(SLAEvent.SLAStatus.class, DBFIELD_SLA_STATUS, FilterComparator.IN),
        EVENT_STATUS(SLAEvent.EventStatus.class, null, null),
        ID(String.class, DBFIELD_JOB_ID, null),
        PARENT_ID(String.class, DBFIELD_PARENT_ID, null),
        BUNDLE(String.class, null, null);

        private final Class type;
        private final String dbFieldName;
        private final FilterComparator filterComparator;
        private static final List<Pair<FilterField, FilterField>> intervalFieldPairs =
                new ArrayList<Pair<FilterField, FilterField>>() {{
            add(new Pair<>(FilterField.NOMINAL_AFTER, FilterField.NOMINAL_BEFORE));
            add(new Pair<>(FilterField.CREATED_AFTER, FilterField.CREATED_BEFORE));
            add(new Pair<>(FilterField.EXPECTEDSTART_AFTER, FilterField.EXPECTEDSTART_BEFORE));
            add(new Pair<>(FilterField.EXPECTEDEND_AFTER, FilterField.EXPECTEDEND_BEFORE));
            add(new Pair<>(FilterField.ACTUALSTART_AFTER, FilterField.ACTUALSTART_BEFORE));
            add(new Pair<>(FilterField.ACTUALEND_AFTER, FilterField.ACTUALEND_BEFORE));
            add(new Pair<>(FilterField.ACTUAL_DURATION_MIN, FilterField.ACTUAL_DURATION_MAX));
            add(new Pair<>(FilterField.EXPECTED_DURATION_MIN, FilterField.EXPECTED_DURATION_MAX));
        }};

        private static final List<Pair<FilterField, FilterField>> deprecatedFieldPairs =
                new ArrayList<Pair<FilterField, FilterField>>() {{
                    add(new Pair<>(FilterField.NOMINAL_START, FilterField.NOMINAL_AFTER));
                    add(new Pair<>(FilterField.NOMINAL_END, FilterField.NOMINAL_BEFORE));
                }};

        FilterField(final Class type, final String dbFieldName, final FilterComparator filterComparator) {
            this.type = type;
            this.dbFieldName = dbFieldName;
            this.filterComparator = filterComparator;
        }

        private static Object convertFromString(final FilterField field, final String valueStr) throws ParseException {
            if (String.class.equals(field.type)) {
                return valueStr;
            }
            else if (Timestamp.class.equals(field.type)) {
                Date date = DateUtils.parseDateUTC(valueStr);
                return new Timestamp(date.getTime());
            }
            else if (Integer.class.equals(field.type)) {
                return Integer.parseInt(valueStr);
            }
            else if (field.type.isEnum()) {
                List<String> list = new ArrayList<>();
                String[] statusArr = valueStr.split(multiValueSeparator);
                for (String s : statusArr) {
                    list.add(Enum.valueOf(field.type, s).toString());
                }
                return list;
            }
            return null;
        }

        private static FilterField findByName(String name) {
            if (name==null || !name.equals(name.toLowerCase(Locale.US))) {
                return null;
            }
            try {
                return FilterField.valueOf(name.toUpperCase(Locale.US));
            } catch (IllegalArgumentException e) {
                return null;
            }
        }

        private String getColumnName() {
            return name().toLowerCase(Locale.US);
        }
    }

    @VisibleForTesting
    static class FilterCollection {
        private final Map<FilterField, Object> filterValues = new LinkedHashMap<>();

        @VisibleForTesting
        void checkAndSetFilterField(String name, String value) throws ServletException, ParseException {
            FilterField field = FilterField.findByName(name);
            if (field == null) {
                throwNewXServletException(ErrorCode.E0401, String.format("Invalid/unsupported names in filter: %s", name));
            }
            else {
                validateAndSetFilterField(findNonDeprecatedFilterField(field), FilterField.convertFromString(field, value));
            }
        }

        @VisibleForTesting
        FilterField findNonDeprecatedFilterField(FilterField maybeDeprecatedFilterField) {
            for (Pair<FilterField, FilterField> pair : FilterField.deprecatedFieldPairs) {
                if (pair.getFirst().equals(maybeDeprecatedFilterField)) {
                    return pair.getSecond();
                }
            }
            return maybeDeprecatedFilterField;
        }

        private void validateAndSetFilterField(FilterField field, Object value) throws ServletException {
            boolean fieldConstraintViolated;
            for (Pair<FilterField, FilterField> pair : FilterField.intervalFieldPairs) {
                String firstColumnName = pair.getFirst().getColumnName();
                String secondColumnName = pair.getSecond().getColumnName();
                if (pair.getFirst().equals(field)) {
                    fieldConstraintViolated = checkFieldConstrantViolation((Comparable)value,
                            (Comparable)getFilterField(secondColumnName));
                }
                else if (pair.getSecond().equals(field)) {
                    fieldConstraintViolated = checkFieldConstrantViolation((Comparable)getFilterField(firstColumnName),
                            (Comparable)value);
                }
                else {
                    fieldConstraintViolated = false;
                }
                if (fieldConstraintViolated) {
                    String errorMessage = String.format("should be: field %s <= field %s", firstColumnName, secondColumnName);
                    throwNewXServletException(ErrorCode.E0302, errorMessage);
                }
            }
            filterValues.put(field, value);
        }

        private void throwNewXServletException(ErrorCode errorCode, String  message) throws ServletException {
            LOG.error(message);
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, errorCode, message);
        }

        private boolean checkFieldConstrantViolation(Comparable minValue, Comparable maxValue) {
            return  minValue != null && maxValue != null && minValue.compareTo(maxValue) > 0;
        }

        @VisibleForTesting
        Object getFilterField(String name) {
            FilterField field = FilterField.findByName(name);
            if (field != null) {
                return filterValues.get(field);
            }
            else {
                return null;
            }
        }
    }

    public SLASummaryGetForFilterJPAExecutor(int numMaxResults) {
        this.filterCollection = new FilterCollection();
        this.numMaxResults = numMaxResults;
    }

    @Override
    public String getName() {
        return SLASummaryGetForFilterJPAExecutor.class.getSimpleName();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<SLASummaryBean> execute(EntityManager em) throws JPAExecutorException {
        initPossibleSortbyColumnList(em);
        createCriteriaQuery(em);
        TypedQuery<SLASummaryBean> typedQuery = em.createQuery(criteriaQuery);
        typedQuery.setMaxResults(numMaxResults);
        LOG.debug("Query string: {0}",typedQuery.unwrap(org.apache.openjpa.persistence.QueryImpl.class).getQueryString());
        return typedQuery.getResultList();
    }

    private void initPossibleSortbyColumnList(EntityManager em) {
        Metamodel metamodel = em.getMetamodel();
        EntityType<SLASummaryBean> slaSummaryBeanEntityType = metamodel.entity(SLASummaryBean.class);
        Set<Attribute<SLASummaryBean,?>> slaSummaryBeanAttributes = slaSummaryBeanEntityType.getDeclaredAttributes();
        possibleSortbyColumns = new ArrayList<>();
        for (Attribute<SLASummaryBean,?> attribute : slaSummaryBeanAttributes) {
            possibleSortbyColumns.add(attribute.getName());
        }
    }

    private void createCriteriaQuery(final EntityManager em) throws JPAExecutorException {
        ensureCriteriaFields(em);
        createSelectFrom();
        createWhereCondition();
        createOrderByClause();
    }

    private void createOrderByClause() throws JPAExecutorException {
        if (Strings.isNullOrEmpty(sortbyColumn)) {
            sortbyColumn = DEFAULT_SORTBY_COLUMN;
        }
        if (!possibleSortbyColumns.contains(sortbyColumn)) {
            String errorMessage = String.format("invalid sortby column: %s", sortbyColumn);
            LOG.error(errorMessage);
            throw new JPAExecutorException(ErrorCode.E0303, errorMessage);
        }

        Path<Object> sortbyColumnPath = root.get(sortbyColumn);
        Path<Object> nominalTimeTSPath = root.get(DBFIELD_NOMINAL_TIME_TS);
        List<Order> orderList = new ArrayList<>();
        if (isDescendingOrder) {
            orderList.add(criteriaBuilder.desc(sortbyColumnPath));
        }
        else {
            orderList.add(criteriaBuilder.asc(sortbyColumnPath));
        }
        if (!DBFIELD_NOMINAL_TIME_TS.equals(sortbyColumn)) {
            orderList.add(criteriaBuilder.asc(nominalTimeTSPath));
        }
        criteriaQuery.orderBy(orderList);
    }

    private void ensureCriteriaFields(EntityManager em) {
        criteriaBuilder = em.getCriteriaBuilder();
        criteriaQuery = criteriaBuilder.createQuery(SLASummaryBean.class);
    }

    private void createSelectFrom() {
        root = criteriaQuery.from(SLASummaryBean.class);
        criteriaQuery.select(root);
    }

    private void createWhereCondition() throws JPAExecutorException {
        preds = new ArrayList<>();
        for (Map.Entry<FilterField, Object> entry : filterCollection.filterValues.entrySet()) {
            FilterField filterField = entry.getKey();
            Object value = entry.getValue();
            if (filterField.filterComparator != null) {
                switch (filterField.filterComparator) {
                    case LIKE:
                        preds.add(criteriaBuilder.like(root.<String>get(filterField.dbFieldName), (String)value));
                        break;
                    case EQUALS:
                        preds.add(criteriaBuilder.equal(root.get(filterField.dbFieldName), value));
                        break;
                    case GREATER_OR_EQUALS:
                        preds.add(criteriaBuilder.greaterThanOrEqualTo(root.get(filterField.dbFieldName), (Comparable)value));
                        break;
                    case LESSTHAN_OR_EQUALS:
                        preds.add(criteriaBuilder.lessThanOrEqualTo(root.get(filterField.dbFieldName), (Comparable)value));
                        break;
                    case IN:
                        preds.add(root.get(filterField.dbFieldName).in((List<String>)value));
                        break;
                }
            }
        }
        createAndAddSpecialCriterias();
        criteriaQuery.where(preds.toArray(new Predicate[0]));
    }

    private void createAndAddSpecialCriterias() throws JPAExecutorException {
        createAndAddIdAndParentIdCriteria();
        createAndAddBundleFilterCriteria();
        createAndAddEventStatusCriteria();
    }

    private void createAndAddIdAndParentIdCriteria() {
        String jobId = (String) getFilterField(DBFIELD_ID);
        String parentId = (String) getFilterField( FilterField.PARENT_ID.getColumnName());

        if (jobId != null && parentId != null) {
            preds.add(criteriaBuilder.or(
                    criteriaBuilder.equal(root.get(FilterField.ID.dbFieldName), jobId),
                    criteriaBuilder.equal(root.get(FilterField.PARENT_ID.dbFieldName), parentId)
            ));
        }
        else if (jobId != null && parentId == null) {
            preds.add(criteriaBuilder.equal(root.get(FilterField.ID.dbFieldName), jobId));
        }
        else if (jobId == null && parentId != null) {
            preds.add(criteriaBuilder.equal(root.get(FilterField.PARENT_ID.dbFieldName), parentId));
        }
    }

    private void createAndAddBundleFilterCriteria() {
        String bundle = (String) getFilterField(FilterField.BUNDLE.getColumnName());

        if (bundle == null) {
            return;
        }
        Subquery<BundleActionBean> subquery = criteriaQuery.subquery(BundleActionBean.class);
        Root<BundleJobBean> subJobBeanRoot = subquery.from(BundleJobBean.class);
        Root<BundleActionBean> subActionBeanRoot = subquery.from(BundleActionBean.class);
        subquery.select(subActionBeanRoot.get(DBFIELD_COORD_ID));
        Predicate bundleJoinPredicate = criteriaBuilder.equal(subActionBeanRoot.get(DBFIELD_BUNDLE_ID),
                subJobBeanRoot.get(DBFIELD_ID));
        if (isBundleId(bundle)) {
            subquery.where(bundleJoinPredicate, criteriaBuilder.equal(subActionBeanRoot.get(DBFIELD_BUNDLE_ID), bundle));
        }
        else {
            subquery.where(bundleJoinPredicate, criteriaBuilder.equal(subJobBeanRoot.get(DBFIELD_APP_NAME), bundle));
        }
        preds.add(criteriaBuilder.in(root.get(DBFIELD_PARENT_ID)).value(subquery));
    }

    private void createAndAddEventStatusCriteria() throws JPAExecutorException {
        String eventStatusFilterFieldName = FilterField.EVENT_STATUS.getColumnName();
        List<String> eventStatusFilterValues = (List<String>)getFilterField(eventStatusFilterFieldName);
        if (eventStatusFilterValues != null) {
            List<Predicate> eventStatusPreds = new ArrayList<>();
            for (String statusStr : eventStatusFilterValues) {
                EventStatus status;
                try {
                    status = SLAEvent.EventStatus.valueOf(statusStr);
                }
                catch (IllegalArgumentException e) {
                    throw new JPAExecutorException(ErrorCode.E0303, eventStatusFilterFieldName, statusStr);
                }
                eventStatusPreds.addAll(EventStatusFilter.createFilterConditionForEventStatus(status, criteriaBuilder, root));
            }
            addEventStatusCriteria(eventStatusPreds);
        }
    }

    private enum EventStatusFilter {
        START_MET_FILTER {
            public List<Predicate> createFilterCondition(CriteriaBuilder criteriaBuilder, Root<SLASummaryBean> root) {
                return Collections.singletonList(criteriaBuilder.and(
                        criteriaBuilder.isNotNull(root.get(DBFIELD_EXPECTED_START_TS)),
                        criteriaBuilder.isNotNull(root.get(DBFIELD_ACTUAL_START_TS)),
                        criteriaBuilder.ge(root.get(DBFIELD_EXPECTED_START_TS), root.get(DBFIELD_ACTUAL_START_TS))));
            }},
        START_MISS_FILTER {
            public List<Predicate> createFilterCondition(CriteriaBuilder criteriaBuilder, Root<SLASummaryBean> root) {
                Timestamp currentTime = new Timestamp(new Date().getTime());
                return Arrays.asList(criteriaBuilder.and(
                        criteriaBuilder.isNotNull(root.get(DBFIELD_EXPECTED_START_TS)),
                        criteriaBuilder.isNotNull(root.get(DBFIELD_ACTUAL_START_TS)),
                        criteriaBuilder.lessThanOrEqualTo(root.get(DBFIELD_EXPECTED_START_TS),
                                root.get(DBFIELD_ACTUAL_START_TS))),
                        criteriaBuilder.and(
                                criteriaBuilder.isNotNull(root.get(DBFIELD_EXPECTED_START_TS)),
                                criteriaBuilder.isNull(root.get(DBFIELD_ACTUAL_START_TS)),
                                criteriaBuilder.lessThanOrEqualTo(root.get(DBFIELD_EXPECTED_START_TS), currentTime)));
            }},
        DURATION_MET_FILTER {
            public List<Predicate> createFilterCondition(CriteriaBuilder criteriaBuilder, Root<SLASummaryBean> root) {
                return Collections.singletonList(criteriaBuilder.and(
                        criteriaBuilder.notEqual(root.get(DBFIELD_EXPECTED_DURATION), -1),
                        criteriaBuilder.notEqual(root.get(DBFIELD_ACTUAL_DURATION), -1),
                        criteriaBuilder.ge(root.get(DBFIELD_EXPECTED_DURATION), root.get(DBFIELD_ACTUAL_DURATION))));
            }},
        DURATION_MISS_FILTER {
            public List<Predicate> createFilterCondition(CriteriaBuilder criteriaBuilder, Root<SLASummaryBean> root) {
                return Arrays.asList(criteriaBuilder.and(
                        criteriaBuilder.notEqual(root.get(DBFIELD_EXPECTED_DURATION), -1),
                        criteriaBuilder.notEqual(root.get(DBFIELD_ACTUAL_DURATION), -1),
                        criteriaBuilder.lessThan(root.get(DBFIELD_EXPECTED_DURATION), root.get(DBFIELD_ACTUAL_DURATION))),
                        criteriaBuilder.equal(root.get(DBFIELD_EVENT_STATUS), EventStatus.DURATION_MISS.name())
                );
            }},
        END_MET_FILTER {
            public List<Predicate> createFilterCondition(CriteriaBuilder criteriaBuilder, Root<SLASummaryBean> root) {
                return Collections.singletonList(criteriaBuilder.and(
                        criteriaBuilder.isNotNull(root.get(DBFIELD_EXPECTED_END_TS)),
                        criteriaBuilder.isNotNull(root.get(DBFIELD_ACTUAL_END_TS)),
                        criteriaBuilder.greaterThanOrEqualTo(root.get(DBFIELD_EXPECTED_END_TS),
                                root.get(DBFIELD_ACTUAL_END_TS))));
            }},
        END_MISS_FILTER {
            public List<Predicate> createFilterCondition(CriteriaBuilder criteriaBuilder, Root<SLASummaryBean> root) {
                Timestamp currentTime = new Timestamp(new Date().getTime());
                return Arrays.asList(criteriaBuilder.and(
                        criteriaBuilder.isNotNull(root.get(DBFIELD_EXPECTED_END_TS)),
                        criteriaBuilder.isNotNull(root.get(DBFIELD_ACTUAL_END_TS)),
                        criteriaBuilder.lessThanOrEqualTo(root.get(DBFIELD_EXPECTED_END_TS),
                                root.get(DBFIELD_ACTUAL_END_TS))),
                        criteriaBuilder.and(
                                criteriaBuilder.isNotNull(root.get(DBFIELD_EXPECTED_END_TS)),
                                criteriaBuilder.isNull(root.get(DBFIELD_ACTUAL_END_TS)),
                                criteriaBuilder.lessThanOrEqualTo(root.get(DBFIELD_EXPECTED_END_TS), currentTime)
                        ));
            }};

        abstract List<Predicate> createFilterCondition(CriteriaBuilder criteriaBuilder, Root<SLASummaryBean> root);

        private static List<Predicate> createFilterConditionForEventStatus(EventStatus eventStatus,
                                                                           CriteriaBuilder criteriaBuilder,
                                                                           Root<SLASummaryBean> root) throws JPAExecutorException {
            try {
                EventStatusFilter eventStatusFilter = EventStatusFilter.valueOf(eventStatus.name() + "_FILTER");
                return eventStatusFilter.createFilterCondition(criteriaBuilder, root);
            }
            catch (IllegalArgumentException e) {
                throw new JPAExecutorException(ErrorCode.E0303, FilterField.EVENT_STATUS.getColumnName(), eventStatus.name());
            }
        }
    }

    private void addEventStatusCriteria(List<Predicate> eventStatusPreds) {
        preds.add(criteriaBuilder.or(eventStatusPreds.toArray(new Predicate[0])));
    }

    public void checkAndSetFilterField(String name, String value) throws ServletException, ParseException {
        filterCollection.checkAndSetFilterField(name, value);
    }

    public void setDescendingOrder(boolean isDescendingOrder) {
        this.isDescendingOrder = isDescendingOrder;
    }

    public void setSortbyColumn(String sortbyColumn) {
        this.sortbyColumn = sortbyColumn;
    }

    @VisibleForTesting
    Object getFilterField(String name) {
        return filterCollection.getFilterField(name);
    }

    private boolean isBundleId(String id) {
        return BUNDLE_ID_PATTERN.matcher(id).matches();
    }
}
