package com.surajpanda.dmq.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RaftMetadataPersistenceTest {

  @TempDir Path tempDir;

  @Test
  void freshNodeStartsAtTermZeroWithNoVote() {
    RaftNode node =
        new RaftNode("broker-1", new FileRaftMetadataStore(tempDir.resolve("meta")), new RaftLog());

    assertEquals(0, node.getCurrentTerm());
    assertNull(node.getVotedFor());
    assertEquals(RaftState.FOLLOWER, node.getState());
  }

  @Test
  void termSurvivesRestart() {
    Path metaFile = tempDir.resolve("meta");

    RaftNode before = new RaftNode("broker-1", new FileRaftMetadataStore(metaFile), new RaftLog());
    before.advanceTerm(7);

    RaftNode after = new RaftNode("broker-1", new FileRaftMetadataStore(metaFile), new RaftLog());

    assertEquals(7, after.getCurrentTerm());
  }

  @Test
  void votedForSurvivesRestart() {
    Path metaFile = tempDir.resolve("meta");

    RaftNode before = new RaftNode("broker-1", new FileRaftMetadataStore(metaFile), new RaftLog());
    before.handleRequestVote(new RequestVoteRequest(3, "broker-2", 0, 0));

    RaftNode after = new RaftNode("broker-1", new FileRaftMetadataStore(metaFile), new RaftLog());

    assertEquals(3, after.getCurrentTerm());
    assertEquals("broker-2", after.getVotedFor());
  }

  @Test
  void restartAfterVotingDoesNotAllowASecondVoteInTheSameTerm() {
    Path metaFile = tempDir.resolve("meta");

    RaftNode before = new RaftNode("broker-1", new FileRaftMetadataStore(metaFile), new RaftLog());
    before.handleRequestVote(new RequestVoteRequest(3, "broker-2", 0, 0));

    // Simulates a crash and restart: a brand new RaftNode instance, same durable file.
    RaftNode after = new RaftNode("broker-1", new FileRaftMetadataStore(metaFile), new RaftLog());

    RequestVoteResponse response =
        after.handleRequestVote(new RequestVoteRequest(3, "broker-3", 0, 0));

    assertFalse(response.voteGranted());
    assertEquals("broker-2", after.getVotedFor());
  }

  @Test
  void restartWithNewerTermRecoversThatTermAndStaysFollower() {
    Path metaFile = tempDir.resolve("meta");

    RaftNode before = new RaftNode("broker-1", new FileRaftMetadataStore(metaFile), new RaftLog());
    before.becomeCandidate();
    before.becomeLeader();
    before.advanceTerm(9); // e.g. observed a higher term from a peer before crashing

    RaftNode after = new RaftNode("broker-1", new FileRaftMetadataStore(metaFile), new RaftLog());

    assertEquals(9, after.getCurrentTerm());
    assertEquals(RaftState.FOLLOWER, after.getState());
  }

  @Test
  void becomingCandidateAfterRestartContinuesFromTheRecoveredTerm() {
    Path metaFile = tempDir.resolve("meta");

    RaftNode before = new RaftNode("broker-1", new FileRaftMetadataStore(metaFile), new RaftLog());
    before.advanceTerm(4);

    RaftNode after = new RaftNode("broker-1", new FileRaftMetadataStore(metaFile), new RaftLog());
    after.becomeCandidate();

    assertEquals(5, after.getCurrentTerm());
  }

  @Test
  void metadataFileIsReadableAcrossFreshFileRaftMetadataStoreInstances() {
    Path metaFile = tempDir.resolve("meta");

    new FileRaftMetadataStore(metaFile).save(2, "broker-9");

    RaftMetadataSnapshot snapshot = new FileRaftMetadataStore(metaFile).load();

    assertEquals(2, snapshot.currentTerm());
    assertEquals("broker-9", snapshot.votedFor());
  }

  @Test
  void loadingWithNoFileYetReturnsInitialSnapshot() {
    RaftMetadataSnapshot snapshot =
        new FileRaftMetadataStore(tempDir.resolve("never-written")).load();

    assertEquals(0, snapshot.currentTerm());
    assertNull(snapshot.votedFor());
  }

  @Test
  void corruptedMetadataFileFailsLoudlyRatherThanBeingIgnored() throws Exception {
    Path metaFile = tempDir.resolve("meta");
    Files.writeString(metaFile, "not-a-valid-record");

    assertThrows(RaftPersistenceException.class, () -> new FileRaftMetadataStore(metaFile).load());
  }

  @Test
  void saveFailsLoudlyWhenTheTargetPathCannotBeWritten() throws Exception {
    // Make the "parent directory" actually a plain file, so Files.createDirectories() must fail.
    Path blockingFile = tempDir.resolve("blocking-file");
    Files.writeString(blockingFile, "not a directory");
    Path unwritableTarget = blockingFile.resolve("meta");

    RaftPersistenceException exception =
        assertThrows(
            RaftPersistenceException.class,
            () -> new FileRaftMetadataStore(unwritableTarget).save(1, "broker-1"));

    assertTrue(exception.getCause() != null);
  }
}
