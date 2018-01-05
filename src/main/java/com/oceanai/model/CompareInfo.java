package com.oceanai.model;


public class CompareInfo {

    private String mId;
    private float similarity = 0;

    public String getmId() {
        return mId;
    }

    public void setmId(String mId) {
        this.mId = mId;
    }

    public float getSimilarity() {
        return similarity;
    }

    public void setSimilarity(float similarity) {
        if (similarity > this.similarity) {
            this.similarity = similarity;
        } else {
            return;
        }
    }
}
