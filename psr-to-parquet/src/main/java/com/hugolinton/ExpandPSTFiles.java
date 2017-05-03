package com.hugolinton;

import com.pff.PSTException;
import com.pff.PSTFile;
import com.pff.PSTFolder;
import com.pff.PSTMessage;

import java.util.ArrayList;
import java.util.Vector;

public class ExpandPSTFiles {
//    public static void main(final String[] args) {
//        new com.hugolinton.ExpandPSTFiles(args[0]);
//    }
//
//    public com.hugolinton.ExpandPSTFiles(final String filename) {
//        try {
//            final PSTFile pstFile = new PSTFile(filename);
//            System.out.println(pstFile.getMessageStore().getDisplayName());
//            this.processFolder(pstFile.getRootFolder());
//        } catch (final Exception err) {
//            err.printStackTrace();
//        }
//    }

    int depth = -1;
    private ArrayList<PSTMessage> messages = new ArrayList<PSTMessage>();

    public void processFolder(final PSTFolder folder) throws PSTException, java.io.IOException {
        this.depth++;
        // the root folder doesn't have a display name
        if (this.depth > 0) {
            this.printDepth();
            System.out.println(folder.getDisplayName());
        }

        // go through the folders...
        if (folder.hasSubfolders()) {
            final Vector<PSTFolder> childFolders = folder.getSubFolders();
            for (final PSTFolder childFolder : childFolders) {
                this.processFolder(childFolder);
            }
        }

        // and now the emails for this folder
        if (folder.getContentCount() > 0) {
            this.depth++;
            PSTMessage email = (PSTMessage) folder.getNextChild();
            while (email != null) {
                this.printDepth();
                PSTMessage email1 = email;
                messages.add(email);
                System.out.println("Email: " + email.getDescriptorNodeId() + " - " + email.getSubject());
                email = (PSTMessage) folder.getNextChild();
            }
            this.depth--;
        }
        this.depth--;
    }

    public ArrayList<PSTMessage> getEmails() {
        return messages;
    }

    public void printDepth() {
        for (int x = 0; x < this.depth - 1; x++) {
            System.out.print(" | ");
        }
        System.out.print(" |- ");
    }
}
