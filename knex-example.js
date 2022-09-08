  const venueQuery = knex("venues")
    .select("lng", "lat", "accountId", "id")
    .whereNull("archivedAt")
    .whereIn("accountId", newPartnerIds);

  if (venueIds?.length) {
    venueQuery.whereIn("id", venueIds);
  }

  const venuesCoordinatesListForPartners = await venueQuery;

  const unscheduledMembersSubQuery = knex("memberConnections")
    .select("memberId")
    .where("memberConnections.accountId", context.locals.user.accountId)
    .whereNotIn("memberId", scheduledMemberIds)
    .where("memberConnections.state", MEMBER_STATES.ACTIVE)
    .as("accessibleMembers");

  if (homeVenueIds?.length) {
    unscheduledMembersSubQuery.whereIn(
      "memberConnections.venueId",
      homeVenueIds
    );
  } else if (userHomeVenueIds?.length) {
    unscheduledMembersSubQuery.whereIn(
      "memberConnections.venueId",
      userHomeVenueIds
    );
  }

  const serviceAreasSubQuery = knex
    .select("id")
    .from("serviceAreas")
    .where("accountId", context.locals.user.accountId)
    .whereNull("archivedAt");

  const unscheduledMembersQuery = knex
    .select("members.*")
    .from(unscheduledMembersSubQuery)
    .leftJoin("members", "members.id", "accessibleMembers.memberId")
    .leftJoin("relationships", function() {
      this.on("relationships.memberId", "accessibleMembers.memberId");
      this.andOnIn("relationships.accountId", newPartnerIds);
    })
    .leftJoin(
      "memberRoles",
      "memberRoles.memberId",
      "accessibleMembers.memberId"
    )
    .leftJoin(
      "memberServiceAreas",
      "memberServiceAreas.memberId",
      "accessibleMembers.memberId"
    )
    .where(function() {
      newPartnerIds.forEach(partnerId =>
        this.orWhere(function() {
          this.whereIn(
            "serviceAreaId",
            serviceAreasSubQuery.clone().where(function() {
              venuesCoordinatesListForPartners
                .filter(
                  coordinatesList =>
                    coordinatesList.accountId === parseInt(partnerId, 10)
                )
                .forEach(coordinates =>
                  this.orWhereRaw("ST_Intersects(ST_MakePoint(?, ?), path)", [
                    coordinates.lng,
                    coordinates.lat
                  ])
                );
            })
          );
          this.whereIn(
            "roleId",
            knex("connections")
              .select("roleRates.roleId")
              .innerJoin(
                "roleRates",
                "roleRates.connectionId",
                "connections.id"
              )
              .whereNull("roleRates.archivedAt")
              .where("targetAccountId", partnerId)
              .where("sourceAccountId", context.locals.user.accountId)
              .whereRaw(`NOW() <= COALESCE("roleRates"."endDate", NOW())`)
          );
        })
      );
    })
    .groupBy("members.id")
    .having(
      knex.raw(
        `count("relationships") = 0 or coalesce(array_length(array_agg(DISTINCT "relationships"."accountId") filter (where "relationships"."poolType"='BLACKLISTED'), 1),0) != ${newPartnerIds.length}`
      )
    );

  if (searchTerm) {
    addSearchTermQuery(unscheduledMembersQuery);
  }

  if (selectedMemberTypes?.length) {
    unscheduledMembersSubQuery.whereIn(
      "memberConnections.memberType",
      selectedMemberTypes
    );
  }

  if (roleIds?.length) {
    unscheduledMembersQuery.whereIn("memberRoles.roleId", roleIds);
  }

  if (venueIds?.length) {
    unscheduledMembersQuery.whereIn(
      "serviceAreaId",
      serviceAreasSubQuery.clone().where(function() {
        venuesCoordinatesListForPartners.forEach(coordinates =>
          this.orWhereRaw("ST_Intersects(ST_MakePoint(?, ?), path)", [
            coordinates.lng,
            coordinates.lat
          ])
        );
      })
    );
  }

  const totalUnscheduledMembersCountQuery = unscheduledMembersQuery.clone();

  const offsetCount = offset ? offset - scheduledMembers.length : 0;
  let limitCount = 0;
  if (limit) {
    limitCount = limit;
    scheduledMembers.length = 0;
  } else if (MEMBER_SOFT_LIMIT > scheduledMembers.length) {
    limitCount = MEMBER_SOFT_LIMIT - scheduledMembers.length;
  }
  unscheduledMembersQuery.offset(offsetCount).limit(limitCount);

  if (sortBy === "FIRSTNAME") {
    scheduledMembers.sort((a, b) =>
      a.member.firstName.localeCompare(b.member.firstName)
    );
    unscheduledMembersQuery.orderBy("members.firstName");
  }
  if (sortBy === "LASTNAME") {
    scheduledMembers.sort((a, b) =>
      a.member.lastName.localeCompare(b.member.lastName)
    );
    unscheduledMembersQuery.orderBy("members.lastName");
  }

  const membersQueryResults = await Promise.all([
    totalUnscheduledMembersCountQuery,
    unscheduledMembersQuery
  ]);
