package org.geogit.di;

public interface Decorator {

    public boolean canDecorate(Object instance);

    public <I> I decorate(I subject);
}
